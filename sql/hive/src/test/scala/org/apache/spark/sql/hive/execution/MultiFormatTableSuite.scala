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

package org.apache.spark.sql.hive.execution

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class MultiFormatTableSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach{
  import testImplicits._

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

//  test("create hive table with parquet data in partitions") {
//    val catalog = spark.sessionState.catalog
//    withTempDir { tmpDir =>
//      val basePath = tmpDir.getCanonicalPath
//      val partitionPath_part1 = new File(basePath + "/dt=2018-01-26")
//      val partitionPath_part2 = new File(basePath + "/dt=2018-01-27")
//      val dirSet =
//        partitionPath_part1 :: partitionPath_part2 :: Nil
//
//      val parquetTable = "ext_parquet_table"
//      val avroTable = "ext_avro_table"
//      val resultTable = "ext_multiformat_partition_table"
//      withTable(parquetTable, avroTable, resultTable) {
//        assert(tmpDir.listFiles.isEmpty)
//        sql(
//          s"""
//             |CREATE EXTERNAL TABLE $parquetTable (key INT, value STRING)
//             |STORED AS PARQUET
//             |LOCATION '${partitionPath_part1.toURI}'
//          """.stripMargin)
//
//        sql(
//          s"""
//             |CREATE EXTERNAL TABLE $avroTable (key INT, value STRING)
//             |STORED AS AVRO
//             |LOCATION '${partitionPath_part2.toURI}'
//          """.stripMargin)
//
//        sql(
//          s"""
//             |CREATE EXTERNAL TABLE $resultTable (key INT, value STRING)
//             |PARTITIONED BY (dt STRING)
//             |STORED AS PARQUET
//             |LOCATION '${tmpDir.toURI}'
//          """.stripMargin)
//
//        sql(
//          s"""
//             |ALTER TABLE $resultTable ADD
//             |PARTITION (dt='2018-01-26') LOCATION '${partitionPath_part1.toURI}'
//             |PARTITION (dt='2018-01-27') LOCATION '${partitionPath_part2.toURI}'
//          """.stripMargin)
//
//        sql(
//          s"""
//             |ALTER TABLE $resultTable PARTITION (dt='2018-01-26') SET FILEFORMAT PARQUET
//          """.stripMargin)
//        sql(
//          s"""
//             |ALTER TABLE $resultTable PARTITION (dt='2018-01-27') SET FILEFORMAT AVRO
//          """.stripMargin)
//
//        // Before data insertion, all the directory are empty
//        assert(dirSet.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))
//
//        sql(
//          s"""
//             |INSERT OVERWRITE TABLE $parquetTable
//             |SELECT 1, 'a'
//          """.stripMargin)
//
//        sql(
//          s"""
//             |INSERT OVERWRITE TABLE $avroTable
//             |SELECT 2, 'b'
//          """.stripMargin)
//
//        val hiveParquetTable =
//          catalog.getTableMetadata(TableIdentifier(parquetTable, Some("default")))
//        assert(DDLUtils.isHiveTable(hiveParquetTable))
//        assert(hiveParquetTable.tableType == CatalogTableType.EXTERNAL)
//        assert(hiveParquetTable.storage.inputFormat ==
//          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
//        assert(hiveParquetTable.storage.outputFormat ==
//          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
//        assert(hiveParquetTable.storage.serde ==
//          Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
//
//        val hiveAvroTable =
//          catalog.getTableMetadata(TableIdentifier(avroTable, Some("default")))
//        assert(DDLUtils.isHiveTable(hiveAvroTable))
//        assert(hiveAvroTable.tableType == CatalogTableType.EXTERNAL)
//        assert(hiveAvroTable.storage.inputFormat ==
//          Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"))
//        assert(hiveAvroTable.storage.outputFormat ==
//          Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
//        assert(hiveAvroTable.storage.serde ==
//          Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
//
//        val hiveResultTable =
//          catalog.getTableMetadata(TableIdentifier(resultTable, Some("default")))
//        assert(DDLUtils.isHiveTable(hiveResultTable))
//        assert(hiveResultTable.tableType == CatalogTableType.EXTERNAL)
//        assert(hiveResultTable.storage.inputFormat ==
//          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
//        assert(hiveResultTable.storage.outputFormat ==
//          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
//        assert(hiveResultTable.storage.serde ==
//          Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
//
//        assert(
//          catalog.listPartitions(TableIdentifier(resultTable,
  // Some("default"))).map(_.spec).toSet ==
//          Set(Map("dt" -> "2018-01-26"), Map("dt"  -> "2018-01-27"))
//        )
//        assert(
//          catalog.getPartition(
//            TableIdentifier(resultTable, Some("default")),
//            Map("dt" -> "2018-01-26")
//          ).storage.serde == Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
//        )
//
//        assert(
//          catalog.getPartition(
//            TableIdentifier(resultTable, Some("default")),
//            Map("dt" -> "2018-01-27")
//          ).storage.serde == Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
//        )
//
//
//        sql(s"DROP TABLE $parquetTable")
//        sql(s"DROP TABLE $avroTable")
//        sql(s"DROP TABLE $resultTable")
//        // drop table will not delete the data of external table
//        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))
//      }
//    }
//  }

  private lazy val parser = new SparkSqlParser(new SQLConf)

  test("create hive table with parquet data in partitions") {
    val catalog = spark.sessionState.catalog
    withTempDir { tmpDir =>
      val basePath = tmpDir.getCanonicalPath
      val partitionPath_part1 = new File(basePath + "/dt=2018-01-26")
      val partitionPath_part2 = new File(basePath + "/dt=2018-01-27")
      val dirSet =
        partitionPath_part1 :: partitionPath_part2 :: Nil

      val resultTable = "ext_multiformat_partition_table"
      withTable(resultTable) {
        assert(tmpDir.listFiles.isEmpty)
        sql(
          s"""
             |CREATE EXTERNAL TABLE $resultTable (key INT, value STRING)
             |PARTITIONED BY (dt STRING)
             |STORED AS PARQUET
             |LOCATION '${tmpDir.toURI}'
          """.stripMargin)

        sql(
          s"""
             |ALTER TABLE $resultTable ADD
             |PARTITION (dt='2018-01-26') LOCATION '${partitionPath_part1.toURI}'
             |PARTITION (dt='2018-01-27') LOCATION '${partitionPath_part2.toURI}'
          """.stripMargin)

        val sql5 = s"""
             |ALTER TABLE $resultTable PARTITION (dt='2018-01-26') SET FILEFORMAT PARQUET
          """.stripMargin

        sql(
          s"""
             |ALTER TABLE $resultTable PARTITION (dt='2018-01-26') SET FILEFORMAT PARQUET
          """.stripMargin)
        sql(
          s"""
             |ALTER TABLE $resultTable PARTITION (dt='2018-01-27') SET FILEFORMAT AVRO
          """.stripMargin)

        // Before data insertion, all the directory are empty
//        assert(dirSet.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))

        val hiveResultTable =
          catalog.getTableMetadata(TableIdentifier(resultTable, Some("default")))
        assert(DDLUtils.isHiveTable(hiveResultTable))
        assert(hiveResultTable.tableType == CatalogTableType.EXTERNAL)
        assert(hiveResultTable.storage.inputFormat ==
          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
        assert(hiveResultTable.storage.outputFormat ==
          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
        assert(hiveResultTable.storage.serde ==
          Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))

        assert(
          catalog.listPartitions(TableIdentifier(resultTable,
            Some("default"))).map(_.spec).toSet ==
            Set(Map("dt" -> "2018-01-26"), Map("dt"  -> "2018-01-27"))
        )

        assert(
          catalog.getPartition(
            TableIdentifier(resultTable, Some("default")),
            Map("dt" -> "2018-01-26")
          ).storage.serde == Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        )

        assert(
          catalog.getPartition(
            TableIdentifier(resultTable, Some("default")),
            Map("dt" -> "2018-01-27")
          ).storage.serde == Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
        )

        sql(s"DROP TABLE $resultTable")
        // drop table will not delete the data of external table
//        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))
      }
    }
  }
}
