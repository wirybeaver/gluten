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

import org.apache.gluten.config.GlutenConfig.COLUMNAR_PARQUET_WRITE_BLOCK_SIZE
import org.apache.gluten.config.VeloxConfig.{MAX_TARGET_FILE_SIZE_SESSION, PARQUET_DICT_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES}

import org.apache.spark.sql.internal.SQLConf

import org.apache.iceberg.TableProperties

import java.util
import java.util.Locale

private[execution] object IcebergWriteOptions {
  def parquetProperties(
      tableProperties: util.Map[String, String],
      writeProperties: util.Map[String, String],
      targetFileSize: Long,
      codec: String): util.HashMap[String, String] = {
    val properties = new util.HashMap[String, String]
    val conf = SQLConf.get
    Seq(
      PARQUET_PAGE_SIZE_BYTES.key -> tableProperties.getOrDefault(
        TableProperties.PARQUET_PAGE_SIZE_BYTES,
        TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT.toString),
      COLUMNAR_PARQUET_WRITE_BLOCK_SIZE.key -> tableProperties.getOrDefault(
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
        TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT.toString),
      MAX_TARGET_FILE_SIZE_SESSION.key -> targetFileSize.toString,
      PARQUET_DICT_SIZE_BYTES.key -> tableProperties.getOrDefault(
        TableProperties.PARQUET_DICT_SIZE_BYTES,
        TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT.toString)
    ).foreach {
      case (key, value) =>
        val capacity = conf.getConfString(key, value).trim
        properties.put(
          key,
          if (capacity.lastOption.exists(_.isDigit)) s"${capacity}B" else capacity)
    }

    Seq(
      "spark.gluten.sql.columnar.backend.velox.parquet_writer_page_row_limit" ->
        tableProperties.getOrDefault(
          TableProperties.PARQUET_PAGE_ROW_LIMIT,
          TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT.toString),
      "spark.gluten.sql.columnar.backend.velox.parquet_writer_datapage_version" ->
        tableProperties.getOrDefault("write.parquet.page-version", "v1").toUpperCase(Locale.ROOT)
    ).foreach { case (key, value) => properties.put(key, conf.getConfString(key, value)) }

    if (Seq("gzip", "zstd").exists(_.equalsIgnoreCase(codec))) {
      val key = "spark.gluten.sql.columnar.backend.velox.parquet_writer_compression_level"
      Option(conf.getConfString(key, null))
        .orElse(Option(writeProperties.get(TableProperties.PARQUET_COMPRESSION_LEVEL)))
        .orElse(Option(tableProperties.get(TableProperties.PARQUET_COMPRESSION_LEVEL)))
        .foreach(level => properties.put(key, level))
    }
    properties
  }
}
