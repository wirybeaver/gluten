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
package org.apache.gluten.connector.write

import org.apache.gluten.proto.IcebergNestedField

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.spark.source.SparkPositionDeltaWriteUtil

import java.util

case class IcebergDeltaDataWriteFactory(
    schema: StructType,
    directory: String,
    codec: String,
    partitionSpec: PartitionSpec,
    field: IcebergNestedField,
    icebergProperties: util.HashMap[String, String],
    queryId: String,
    nativeWriteInfo: Array[Byte],
    specs: util.Map[Integer, PartitionSpec],
    fileIO: FileIO,
    referencedDataFilesBroadcast: Broadcast[util.Map[
      String,
      SparkPositionDeltaWriteUtil.ReferencedDataFile]])
  extends ColumnarBatchDataWriterFactory
  with ColumnarStreamingDataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] =
    createWriter(partitionId, taskId, 0)

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    val (writerHandle, jniWrapper) = IcebergDataWriteFactory.createNativeWriter(
      schema,
      1,
      directory,
      codec,
      partitionId,
      taskId,
      queryId + "-" + epochId,
      partitionSpec,
      field,
      icebergProperties,
      nativeWriteInfo)
    IcebergDeltaColumnarBatchDataWriter(
      writerHandle,
      jniWrapper,
      specs,
      fileIO,
      referencedDataFilesBroadcast)
  }
}
