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

import java.io.{BufferedInputStream, File, FileInputStream}
import java.net.URI

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class MultiFormatTableSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach{
  import testImplicits._

  val parser = new SparkSqlParser(new SQLConf())

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

  val partitionCol = "dt"
  val partitionVal1 = "2018-01-26"
  val partitionVal2 = "2018-01-27"
  private case class PartitionDefinition(
    column: String,
    value: String,
    location: Option[URI] = None,
    format: Option[String] = None
  ) {
    def toSpec: String = {
      s"($column='$value')"
    }
    def toSpecAsMap: Map[String, String] = {
      Map(column -> value)
    }
  }

  test("create hive table with multi format partitions") {
    val catalog = spark.sessionState.catalog
    withTempDir { baseDir =>
      val basePath = baseDir.getCanonicalPath
      val partitionPath_part1 = new File(basePath + s"/$partitionCol=$partitionVal1")
      val partitionPath_part2 = new File(basePath + s"/$partitionCol=$partitionVal2")
      val allDirs =
        baseDir :: partitionPath_part1 :: partitionPath_part2 :: Nil
      val partitionDirs =
        partitionPath_part1 :: partitionPath_part2 :: Nil

      val resultTable = "ext_multiformat_partition_table"
      withTable(resultTable) {
        assert(baseDir.listFiles.isEmpty)

        createExternalTable(resultTable, baseDir.toURI)

        // Before partition creation, all the directory are empty
        assert(allDirs.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))

        val partitions = List(
          PartitionDefinition(
            partitionCol, partitionVal1, Some(partitionPath_part1.toURI), format = Some("PARQUET")
          ),
          PartitionDefinition(
            partitionCol, partitionVal2, Some(partitionPath_part2.toURI), format = Some("AVRO")
          )
        )
        addPartitions(resultTable, partitions)
        // After partition creation, all the partition directories are empty
        assert(partitionDirs.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))
        // baseDir is not (contains the partition dirs)
        assert(baseDir.listFiles().nonEmpty)
        assert(baseDir.listFiles().toSeq == partitionDirs)

        setPartitionFormat(resultTable, partitions.head)
        setPartitionFormat(resultTable, partitions.last)

        // Check table storage type is PARQUET
        val hiveResultTable =
          catalog.getTableMetadata(TableIdentifier(resultTable, Some("default")))
        assert(DDLUtils.isHiveTable(hiveResultTable))
        assert(hiveResultTable.tableType == CatalogTableType.EXTERNAL)
        assert(hiveResultTable.storage.inputFormat
          .contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
        )
        assert(hiveResultTable.storage.outputFormat
          .contains("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
        )
        assert(hiveResultTable.storage.serde
          .contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        )

        // Check table has correct partititons
        assert(
          catalog.listPartitions(TableIdentifier(resultTable,
            Some("default"))).map(_.spec).toSet == partitions.map(_.toSpecAsMap).toSet
        )

        // Check first table partition storage type is PARQUET
        assert(
          catalog.getPartition(
            TableIdentifier(resultTable, Some("default")),
            partitions.head.toSpecAsMap
          ).storage.serde.contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        )

        // Check second table partition storage type is AVRO
        assert(
          catalog.getPartition(
            TableIdentifier(resultTable, Some("default")),
            partitions.last.toSpecAsMap
          ).storage.serde.contains("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
        )

      }
    }
  }

  test("create hive table with multi format partitions containing correct data") {
    val catalog = spark.sessionState.catalog
    withTempDir { baseDir =>
      val basePath = baseDir.getCanonicalPath
      val partitionPath_part1 = new File(basePath + s"/$partitionCol=$partitionVal1")
      val partitionPath_part2 = new File(basePath + s"/$partitionCol=$partitionVal2")
      val allDirs =
        baseDir :: partitionPath_part1 :: partitionPath_part2 :: Nil
      val partitionDirs =
        partitionPath_part1 :: partitionPath_part2 :: Nil

      val resultTable = "ext_multiformat_partition_table_with_data"
      val avroPartitionTable = "ext_avro_partition_table"
      withTable(resultTable, avroPartitionTable) {
        assert(baseDir.listFiles.isEmpty)

        createExternalTable(resultTable, baseDir.toURI)

        // Before partition creation, all the directory are empty
        assert(allDirs.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))

        val partitions = List(
          PartitionDefinition(
            partitionCol, partitionVal1, Some(partitionPath_part1.toURI), format = Some("PARQUET")
          ),
          PartitionDefinition(
            partitionCol, partitionVal2, Some(partitionPath_part2.toURI), format = Some("AVRO")
          )
        )

        addPartitions(resultTable, partitions)
        // After partition creation, all the partition directories are empty
        assert(partitionDirs.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))
        // baseDir is not (contains the partition dirs)
        assert(baseDir.listFiles().nonEmpty)
        assert(baseDir.listFiles().toSeq == partitionDirs)

        setPartitionFormat(resultTable, partitions.head)
        setPartitionFormat(resultTable, partitions.last)

        // INSERT OVERWRITE TABLE only works for the default table format.
        // So we can use it here to insert data into the parquet partition
        sql(
          s"""
             |INSERT OVERWRITE TABLE $resultTable
             |PARTITION ${partitions.head.toSpec}
             |SELECT 1 as id, 'a' as value
                  """.stripMargin)

        val parquetData = spark.read.parquet(partitionPath_part1.getCanonicalPath)
        checkAnswer(parquetData, Row(1, "a"))

        // The only valid way to insert avro data into the avro partition
        // is to create a new avro table directly on the location of the avro partition
        val avroSchema =
          """{
            |  "name": "baseRecord",
            |  "type": "record",
            |  "fields": [{
            |    "name": "key",
            |    "type": ["null", "int"],
            |    "default": null
            |  },
            |  {
            |    "name": "value",
            |    "type": ["null", "string"],
            |    "default": null
            |  }]
            |}
          """.stripMargin

        // Creates the Avro table
        sql(
          s"""
             |CREATE TABLE $avroPartitionTable
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '${partitionPath_part2.getCanonicalPath}'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
            """.stripMargin
        )

        sql(
          s"""
             |INSERT OVERWRITE TABLE $avroPartitionTable
             |SELECT 2, 'b'
           """.stripMargin
        )

        // Directly reading from the avro table should yield correct results
        val avroData = spark.read.table(avroPartitionTable)
        checkAnswer(avroData, Row(2, "b"))

        // Selecting data from the partition currently fails because it tries to
        // read avro data with parquet reader
        val avroPartitionData = sql(
          s"""
             |SELECT key, value FROM ${resultTable}
             |WHERE ${partitionCol}='${partitionVal2}'
           """.stripMargin
        )
        checkAnswer(avroPartitionData, Row(2, "b"))

      }
    }
  }

  private def createExternalTable(table: String, location: URI): DataFrame = {
    sql(
      s"""
         |CREATE EXTERNAL TABLE $table (key INT, value STRING)
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
         |LOCATION '$location'
      """.stripMargin
    )
  }

  private def addPartitions(table: String, partitionDefs: List[PartitionDefinition]): DataFrame = {
    val partitions = partitionDefs
      .map(definition => s"PARTITION ${definition.toSpec} LOCATION '${definition.location.get}'")
      .mkString("\n")

    sql(
      s"""
         |ALTER TABLE $table ADD
         |$partitions
      """.stripMargin
    )

  }

  private def setPartitionFormat(
                                  table: String,
                                  partitionDef: PartitionDefinition
                                ): DataFrame = {
    sql(
      s"""
         |ALTER TABLE $table
         |PARTITION ${partitionDef.toSpec} SET FILEFORMAT ${partitionDef.format.get}
      """.stripMargin
    )
  }
}
