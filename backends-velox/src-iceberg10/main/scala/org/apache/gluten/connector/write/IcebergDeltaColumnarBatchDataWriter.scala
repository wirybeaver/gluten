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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.IcebergWriteJniWrapper

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.spark.source.SparkPositionDeltaWriteUtil

import java.util

case class IcebergDeltaColumnarBatchDataWriter(
    writer: Long,
    jniWrapper: IcebergWriteJniWrapper,
    specs: util.Map[Integer, PartitionSpec],
    fileIO: FileIO,
    referencedDataFilesBroadcast: Broadcast[util.Map[
      String,
      SparkPositionDeltaWriteUtil.ReferencedDataFile]])
  extends DataWriter[ColumnarBatch]
  with Logging {

  private var nativeCommitted = false
  private var aborted = false
  private var closed = false

  override def write(batch: ColumnarBatch): Unit = {
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    jniWrapper.write(writer, batchHandle)
  }

  override def commit(): WriterCommitMessage = {
    val nativeCommitMessages = jniWrapper.commit(writer)
    nativeCommitted = true
    try {
      SparkPositionDeltaWriteUtil.toDeltaTaskCommit(
        specs,
        nativeCommitMessages,
        referencedDataFilesBroadcast.value)
    } catch {
      case cause: Throwable =>
        SparkPositionDeltaWriteUtil.deleteNativeFiles(fileIO, nativeCommitMessages, cause)
        throw cause
    }
  }

  override def abort(): Unit = {
    if (!aborted && !closed && !nativeCommitted) {
      logInfo("Abort the Iceberg delta columnar writer")
      jniWrapper.abort(writer)
      aborted = true
    }
  }

  override def close(): Unit = {
    if (!closed) {
      logDebug("Close the Iceberg delta columnar writer")
      jniWrapper.close(writer)
      closed = true
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    jniWrapper.metrics(writer).toCustomTaskMetrics
  }

}
