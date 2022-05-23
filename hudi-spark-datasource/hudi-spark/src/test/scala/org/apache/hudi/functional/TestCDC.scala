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

import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.{DataSourceWriteOptions, HoodieDataSourceHelpers, SparkDatasetMixin}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.JavaConversions._

class TestCDC extends HoodieClientTestBase with SparkDatasetMixin {

  var spark: SparkSession = _

  private val log = LogManager.getLogger(classOf[TestCDC])

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "version",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    "hoodie.table.cdf.enabled" -> "true"
  )

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test def testCOWBasic() {
    val _spark = spark
    import _spark.implicits._
    val inputDF = Seq((1, "zhang3", 1000L, Map("k1" -> "v1")), (2, "li4", 1000L, Map("k1" -> "v2")))
      .toDF("id", "name", "version", "config")
//    inputDF.write.format("hudi")
//      .options(commonOpts)
//      .mode(SaveMode.Overwrite)
//      .save(basePath)

    val rows = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "cdc")
//      .option("hoodie.datasource.read.begin.instanttime", "20220428140802491")
//        .load("file:////Users/xunjing/hudi/hudi_cdc_cow_nonpt1")
      .option("hoodie.datasource.read.begin.instanttime", "20220523145053942")
      .load("file:////Users/xunjing/hudi/hudi_cdc_mor_nonpt1")
    rows.show(false)
    assertTrue(rows.count() == 6)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }
}
