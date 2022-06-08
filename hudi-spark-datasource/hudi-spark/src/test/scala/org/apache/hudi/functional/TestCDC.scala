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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceWriteOptions, SparkDatasetMixin}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.JavaConversions._

class TestCDC extends HoodieClientTestBase with SparkDatasetMixin {

  var spark: SparkSession = _

  val commonOpts = Map(
    "hoodie.table.cdf.enabled" -> "true",
    "hoodie.insert.shuffle.parallelism" -> "1",
    "hoodie.upsert.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "version",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test def testCOWBasic(): Unit = {
//    val _spark = spark
//    import _spark.implicits._
//    val inputDF = Seq((1, "zhang3", 1000L, Map("k1" -> "v1")), (2, "li4", 1000L, Map("k1" -> "v2")))
//      .toDF("id", "name", "version", "config")
//    inputDF.write.format("hudi")
//      .options(commonOpts)
//      .mode(SaveMode.Overwrite)
//      .save(basePath)

    val rows = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "cdc")
      .option("hoodie.datasource.read.begin.instanttime", "20220608153803713")
      .load("file:///Users/xunjing/hudi/hudi_cdc_file_mor_2")
//      .option("hoodie.datasource.read.begin.instanttime", "20220607145959936")
//      .load("file:///Users/xunjing/hudi/hudi_cdc_file_test_1")
//      .option("hoodie.datasource.read.begin.instanttime", "20220527232140915")
//      .load("file:///Users/xunjing/hudi/hudi_cdc_mor_pt1")
//      .option("hoodie.datasource.read.begin.instanttime", "20220527174932731")
//      .load("file:///Users/xunjing/hudi/hudi_cdc_cow_pt1")
//      .option("hoodie.datasource.read.begin.instanttime", "20220428140802491")
//        .load("file:///Users/xunjing/hudi/hudi_cdc_cow_nonpt1")
//      .option("hoodie.datasource.read.begin.instanttime", "20220523145053941")
//      .load("file:///Users/xunjing/hudi/hudi_cdc_mor_nonpt1")
//        .option("hoodie.datasource.read.begin.instanttime", "20220527232309507")
//        .option("hoodie.datasource.read.end.instanttime", "20220527232727644")
//        .load("file:///Users/xunjing/hudi/hudi_cdc_mor_pt2")
    rows.show(false)
//    println(rows.where("op = 'd'").count())
  }

  @Test
  def testCDCLogFile(): Unit = {
    val _spark = spark
    import _spark.implicits._

    val dataPath = "file:///Users/xunjing/hudi/hudi_cdc_file_test_3"

    val inputDF1 = Seq((1, "zhang3", 1000L, Map("k1" -> "v1")), (2, "li4", 1000L, Map("k1" -> "v2")))
      .toDF("id", "name", "version", "config")
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(dataPath)

    val inputDF2 = Seq((2, "li4_new", 1100L, Map("k1" -> "v22")), (3, "wang5", 1100L, Map("k1" -> "v3")))
      .toDF("id", "name", "version", "config")
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(dataPath)
  }

  @Test
  def testMORCDCLogFile(): Unit = {
    val _spark = spark
    import _spark.implicits._

//    val df = spark.read.format("hudi").load("file:////Users/xunjing/hudi/hudi_cdc_file_mor_2")
//    df.show(false)

    val dataPath = "file:///Users/xunjing/hudi/hudi_cdc_file_mor_2"
//    return

    val inputDF1 = Seq((1, "zhang3", 1000L), (2, "li4", 1000L)).toDF("id", "name", "version")
    inputDF1.write.format("hudi").options(commonOpts).option("hoodie.datasource.write.table.type", "MERGE_ON_READ").mode(SaveMode.Overwrite).save(dataPath)

    val inputDF2 = Seq((2, "li4_new", 1100L)).toDF("id", "name", "version")
    inputDF2.write.format("hudi").options(commonOpts).option("hoodie.datasource.write.table.type", "MERGE_ON_READ").mode(SaveMode.Append).save(dataPath)

    val inputDF3 = Seq((3, "wang5_new", 1200L)).toDF("id", "name", "version")
    inputDF3.write.format("hudi").options(commonOpts).option("hoodie.datasource.write.table.type", "MERGE_ON_READ").mode(SaveMode.Append).save(dataPath)

    val inputDF4 = Seq((3, "wang5_new", 1300L), (4, "x6", 1300L)).toDF("id", "name", "version")
    inputDF4.write.format("hudi").options(commonOpts).option("hoodie.datasource.write.table.type", "MERGE_ON_READ").mode(SaveMode.Append).save(dataPath)
  }

}
