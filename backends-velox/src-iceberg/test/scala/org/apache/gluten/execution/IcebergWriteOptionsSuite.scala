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

import org.apache.spark.sql.internal.SQLConf

import org.apache.iceberg.TableProperties
import org.scalatest.funsuite.AnyFunSuite

import java.util

class IcebergWriteOptionsSuite extends AnyFunSuite {
  test("Parquet options honor session overrides and retain Iceberg table settings") {
    val table = new util.HashMap[String, String]
    table.put(TableProperties.PARQUET_COMPRESSION_LEVEL, "1")
    table.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "8192")
    table.put("write.parquet.page-version", "v2")
    val write = new util.HashMap[String, String]
    write.put(TableProperties.PARQUET_COMPRESSION_LEVEL, "3")
    val compressionKey =
      "spark.gluten.sql.columnar.backend.velox.parquet_writer_compression_level"
    val conf = new SQLConf

    SQLConf.withExistingConf(conf) {
      val options = IcebergWriteOptions.parquetProperties(table, write, 16384, "gzip")
      assert(options.get(compressionKey) == "3")
      assert(options.get(COLUMNAR_PARQUET_WRITE_BLOCK_SIZE.key) == "8192B")
      assert(options.get(
        "spark.gluten.sql.columnar.backend.velox.parquet_writer_datapage_version") == "V2")
      conf.setConfString(compressionKey, "5")
      assert(IcebergWriteOptions.parquetProperties(table, write, 16384, "gzip")
        .get(compressionKey) == "5")
    }
  }
}
