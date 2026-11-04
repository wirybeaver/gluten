/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.gluten.IcebergNestedFieldVisitor
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, ColumnarStreamingDataWriterFactory, IcebergDeltaDataWriteFactory}
import org.apache.gluten.proto.{IcebergExistingDeletionVector, IcebergNativeWriteInfo, IcebergNativeWriteMode}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DeltaWrite, RowLevelOperation}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.WriteDeltaExec
import org.apache.spark.sql.types._

import org.apache.iceberg.{DeleteFile, PartitionSpec, Schema, TableProperties}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.{IcebergWriteUtil, SparkPositionDeltaWriteUtil}
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.TypeUtil

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

case class VeloxIcebergWriteDeltaExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: DeltaWrite,
    @transient writeInfo: SparkPositionDeltaWriteUtil.WriteInfo,
    nativeDataSchema: StructType,
    nativeWriteInfo: Array[Byte],
    referencedDataFiles: util.Map[String, SparkPositionDeltaWriteUtil.ReferencedDataFile])
  extends ColumnarV2TableWriteExec {

  @transient private var referencedDataFilesBroadcast: Broadcast[util.Map[
    String,
    SparkPositionDeltaWriteUtil.ReferencedDataFile]] = _

  private lazy val table = writeInfo.table()
  private lazy val icebergWriteSchema: Schema =
    Option(writeInfo.dataSchema()).getOrElse(table.schema())

  override protected def createBatchWriterFactory(
      schema: StructType): ColumnarBatchDataWriterFactory = {
    createDataWriteFactory()
  }

  override protected def createStreamingWriterFactory(
      schema: StructType): ColumnarStreamingDataWriterFactory = {
    createDataWriteFactory()
  }

  private def createDataWriteFactory(): IcebergDeltaDataWriteFactory = {
    val nestedField = TypeUtil.visit(icebergWriteSchema, new IcebergNestedFieldVisitor)
    val properties = IcebergWriteOptions.parquetProperties(
      table.properties(),
      writeInfo.writeProperties(),
      writeInfo.targetDataFileSize(),
      parquetCodec)

    IcebergDeltaDataWriteFactory(
      nativeDataSchema,
      table.locationProvider().newDataLocation("").stripSuffix("/"),
      parquetCodec,
      table.spec(),
      nestedField,
      properties,
      writeInfo.queryId(),
      nativeWriteInfo,
      table.specs(),
      table.io(),
      getReferencedDataFilesBroadcast
    )
  }

  private def getReferencedDataFilesBroadcast
      : Broadcast[util.Map[String, SparkPositionDeltaWriteUtil.ReferencedDataFile]] = synchronized {
    if (referencedDataFilesBroadcast == null) {
      referencedDataFilesBroadcast = writeInfo.sparkContext().broadcast(referencedDataFiles)
    }
    referencedDataFilesBroadcast
  }

  private def tableProperty(key: String, defaultValue: String): String = {
    Option(writeInfo.writeProperties().get(key))
      .orElse(Option(table.properties().get(key)))
      .getOrElse(defaultValue)
  }

  private def parquetCodec: String = {
    val codec = tableProperty(
      TableProperties.PARQUET_COMPRESSION,
      TableProperties.PARQUET_COMPRESSION_DEFAULT)
    if (codec.equalsIgnoreCase("uncompressed")) "none" else codec.toLowerCase(Locale.ROOT)
  }

  override def doValidateInternal(): ValidationResult = {
    IcebergWriteUtil.validateDataWrite(
      icebergWriteSchema,
      parquetCodec,
      query.output.map(_.name)) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    if (table.sortOrder().isSorted) {
      return ValidationResult.failed("Native Iceberg mutation write does not support sort order")
    }
    if (
      referencedDataFiles
        .values()
        .asScala
        .exists(_.spec().specId() != table.spec().specId())
    ) {
      return ValidationResult.failed(
        "Native Iceberg mutation write does not support files from an older partition spec")
    }
    VeloxIcebergWriteDeltaExec.validatePartitionSpec(table.spec()) match {
      case Some(reason) => return ValidationResult.failed(reason)
      case None =>
    }
    BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema) match {
      case Some(reason) => ValidationResult.failed(reason)
      case None => ValidationResult.succeeded
    }
  }

  override protected def run(): Seq[InternalRow] = {
    try {
      super.run()
    } finally {
      synchronized {
        if (referencedDataFilesBroadcast != null) {
          referencedDataFilesBroadcast.destroy()
          referencedDataFilesBroadcast = null
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(query = newChild)
}

object VeloxIcebergWriteDeltaExec {
  def apply(original: WriteDeltaExec): Option[VeloxIcebergWriteDeltaExec] = {
    if (!SparkPositionDeltaWriteUtil.supportsWrite(original.write)) {
      return None
    }
    val info = SparkPositionDeltaWriteUtil.writeInfo(original.write)
    if (SparkPositionDeltaWriteUtil.validate(info) != null) {
      return None
    }
    val referencedFiles = SparkPositionDeltaWriteUtil.referencedDataFiles(info)
    if (!referencedFiles.isPresent) {
      return None
    }
    val referenced = referencedFiles.get()

    val mode = writeMode(info.command())
    val dataSchema = nativeDataSchema(info, mode)
    val existingDeletionVectors = referenced
      .values()
      .asScala
      .flatMap(ref => Option(ref.existingDeletionVector()))
      .toSeq
    val nativeInfo = buildNativeWriteInfo(
      info.command(),
      original.query.schema,
      dataSchema,
      info.deleteSparkType(),
      existingDeletionVectors)

    Some(
      VeloxIcebergWriteDeltaExec(
        original.query,
        original.refreshCache,
        original.write,
        info,
        dataSchema,
        nativeInfo.toByteArray,
        referenced))
  }

  private[execution] def buildNativeWriteInfo(
      command: RowLevelOperation.Command,
      inputSchema: StructType,
      dataSchema: StructType,
      deleteSchema: StructType,
      existingDeletionVectors: Seq[DeleteFile]): IcebergNativeWriteInfo = {
    val mode = writeMode(command)
    val dataChannels =
      if (mode == IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR) {
        Seq.empty
      } else {
        dataSchema.fields.map(field => inputSchema.fieldIndex(field.name)).toSeq
      }
    val nativeInfo = IcebergNativeWriteInfo
      .newBuilder()
      .setWriteMode(mode)
      .addAllDataColumnIndices(dataChannels.map(Int.box).asJava)
      .setOperationColumnIndex(0)
      .setFilePathColumnIndex(inputSchema.fieldIndex(deleteSchema.fields(0).name))
      .setRowPositionColumnIndex(inputSchema.fieldIndex(deleteSchema.fields(1).name))

    existingDeletionVectors.foreach {
      deletionVector =>
        nativeInfo.addExistingDeletionVectors(toExistingDeletionVector(deletionVector))
    }
    nativeInfo.build()
  }

  private def writeMode(command: RowLevelOperation.Command): IcebergNativeWriteMode = {
    command match {
      case RowLevelOperation.Command.DELETE =>
        IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR
      case RowLevelOperation.Command.UPDATE | RowLevelOperation.Command.MERGE =>
        IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_MERGE
      case other =>
        throw new IllegalArgumentException(s"Unsupported Iceberg row-level command: $other")
    }
  }

  private def toExistingDeletionVector(
      deletionVector: DeleteFile): IcebergExistingDeletionVector = {
    IcebergExistingDeletionVector
      .newBuilder()
      .setReferencedDataFile(deletionVector.referencedDataFile())
      .setPuffinPath(deletionVector.location().toString)
      .setContentOffset(deletionVector.contentOffset())
      .setContentLength(deletionVector.contentSizeInBytes())
      .setRecordCount(deletionVector.recordCount())
      .setFileSizeInBytes(deletionVector.fileSizeInBytes())
      .build()
  }

  private def nativeDataSchema(
      info: SparkPositionDeltaWriteUtil.WriteInfo,
      mode: IcebergNativeWriteMode): StructType = {
    if (mode == IcebergNativeWriteMode.ICEBERG_NATIVE_WRITE_MODE_DELETION_VECTOR) {
      SparkSchemaUtil.convert(info.table().schema())
    } else {
      info.dataSparkType()
    }
  }

  private[execution] def validatePartitionSpec(spec: PartitionSpec): Option[String] = {
    if (spec.isPartitioned) {
      val topLevelIds = spec.schema().columns().asScala.map(_.fieldId()).toSet
      if (
        spec
          .fields()
          .asScala
          .exists(
            field =>
              !isSupportedPartitionType(spec, field) ||
                !topLevelIds.contains(field.sourceId()) ||
                field.transform().isVoid)
      ) {
        return Some(
          "Native Iceberg mutation write does not support this partition type, " +
            "void transforms, or nested partition columns")
      }
    }
    None
  }

  private def isSupportedPartitionType(
      spec: PartitionSpec,
      field: org.apache.iceberg.PartitionField): Boolean = {
    val typeId = spec.schema().findType(field.sourceId()).typeId()
    typeId != TypeID.DOUBLE && typeId != TypeID.FLOAT
  }
}
