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

package org.apache.hudi.functional.cdc

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{Column, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{Add, If, Literal}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._

import org.junit.jupiter.api.Test

class TestCDCStreamingSuite extends TestCDCBase {

  /**
   * three tables:
   *  -- source_tbl(id, country), a upstream source;
   *  -- detail_tbl(id, name, country), as a DWD table;
   *  -- country_tbl(country, cnt), as a DWS table, used to count the permanent population of a country.
   *
   *  Here test a pipeline from ODS to DWD, to DWS.
   *  Between ODS and DWD. merge into hudi directly.
   *  Between DWD and DWS. use agg and merge into hudi.
   */
  @Test
  def cdcStreaming(): Unit = {
    val commonOptions = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1"
    )

    val _spark = spark
    import _spark.implicits._

    val userToCountryTblPath = s"$basePath/user_to_country_table"
    val countryToPopulationTblPath = s"$basePath/country_to_population_table"

    // define detail_tbl as a table that enable CDC.
    // assume that there already are three records in detail_tbl.
    val userToCountryDF = Seq(
      (1, "US", "1000"), (2, "US", "1000"),
      (3, "China", "1000"), (4, "Singapore", "1000")
    ).toDF("userid", "country", "ts")
    userToCountryDF.write.format("hudi")
      .options(commonOptions)
      .option(HoodieTableConfig.CDC_ENABLED.key, "true")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "userid")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(HoodieWriteConfig.TBL_NAME.key, "user_to_country")
      .save(userToCountryTblPath)

    // assume that there already are three records in country_tbl.
    val countryToPopulationDF = Seq(
      ("US", 200, "1000"), ("China", 50, "1000"), ("Singapore", 20, "1000")
    ).toDF("country", "population", "ts")
    countryToPopulationDF.write.format("hudi")
      .options(commonOptions)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "country")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(HoodieWriteConfig.TBL_NAME.key, "country_to_population")
      .save(countryToPopulationTblPath)

    val hadoopConf = spark.sessionState.newHadoopConf()
    val userToCountryMetaClient = HoodieTableMetaClient.builder()
      .setBasePath(userToCountryTblPath)
      .setConf(hadoopConf)
      .build()

    val inputData = new MemoryStream[(Int, String, String)](100, spark.sqlContext)
    val df = inputData.toDS().toDF("userid", "country", "ts")
    // stream1: from ods to dwd
    val stream1 = df.writeStream
      .format("hudi")
      .foreachBatch{ (batch, _) =>
        batch.write.format("hudi")
          .options(commonOptions)
          .option(HoodieTableConfig.CDC_ENABLED.key, "true")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "userid")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
          .option(HoodieWriteConfig.TBL_NAME.key, "user_to_country")
          .mode(SaveMode.Append)
          .save(userToCountryTblPath)
      }
      .start()

    // stream2: from dwd to dws
    val dec = typedLit(-1).expr
    val inc = typedLit(1).expr
    val zero = typedLit(0).expr
    val beforeCntExpr = If(isnull(col("bcountry")).expr, zero, dec)
    val afterCntExpr = If(isnull(col("acountry")).expr, zero, inc)
    val stream2 = spark.readStream.format("hudi")
      .option("hoodie.datasource.query.type", "cdc")
      .load(userToCountryTblPath)
      .writeStream
      .format("hudi")
      .foreachBatch{ (batch, _) =>
        val current = spark.read.format("hudi").load(countryToPopulationTblPath)
        batch
          .select(
            get_json_object(col("before"), "$.country").as("bcountry"),
            get_json_object(col("after"), "$.country").as("acountry"),
            get_json_object(col("after"), "$.ts").as("ts")
          )
          .withColumn("bcnt", new Column(beforeCntExpr)).withColumn("acnt", new Column(afterCntExpr))
          .select(
            explode(array(Array(
              struct(col("bcountry").as("country"), col("bcnt").as("cnt"), col("ts")),
              struct(col("acountry").as("country"), col("acnt").as("cnt"), col("ts"))): _*))
          )
          .select(col("col.country").as("country"), col("col.cnt").as("cnt"), col("col.ts").as("ts"))
          .where("country is not null").groupBy("country")
          .agg(("cnt" -> "sum"), ("ts" -> "max"))
          .join(current, Seq("country"), "left")
          .select(
            col("country"),
            new Column(
              Add(col("sum(cnt)").expr, If(isnull(col("population")).expr, Literal(0), col("population").expr))).as("population"),
            col("max(ts)").as("ts")
          )
          .write.format("hudi")
          .options(commonOptions)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "country")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
          .option(HoodieWriteConfig.TBL_NAME.key, "country_to_population")
          .mode(SaveMode.Append)
          .save(countryToPopulationTblPath)
      }
      .start()

    // fake upstream batch1
    inputData.addData(Seq((3, "US", "1100"), (4, "US", "1100"), (5, "US", "1100")))
    stream1.processAllAvailable()
    stream2.processAllAvailable()

    val detailOutput1 = spark.read.format("hudi").load(userToCountryTblPath)
    assert(detailOutput1.where("country = 'US'").count() == 5)
    val ucTs1 = userToCountryMetaClient.reloadActiveTimeline().lastInstant.get.getTimestamp
    val ucDdcData1 = cdcDataFrame(userToCountryTblPath, (ucTs1.toLong - 1).toString, null)
    ucDdcData1.show(false)
    assertCDCOpCnt(ucDdcData1, 1, 2, 0)

    val countryRes1 = Seq(
      Row("China", 50),
      Row("Singapore", 20),
      Row("US", 205)
    )
    var currentCP = spark.read.format("hudi")
      .load(countryToPopulationTblPath)
      .select("country", "population")
      .sort("country")
    checkAnswer(currentCP, countryRes1)

    // fake upstream batch2
    inputData.addData(Seq((3, "Singapore", "1200"), (7, "Canada", "1200"), (8, "Singapore", "1200")))
    stream1.processAllAvailable()
    stream2.processAllAvailable()

    val ts2 = userToCountryMetaClient.reloadActiveTimeline().lastInstant.get.getTimestamp
    val cdcData2 = cdcDataFrame(userToCountryTblPath, (ts2.toLong - 1).toString, null)
    cdcData2.show(false)
    assertCDCOpCnt(cdcData2, 2, 1, 0)

    val countryRes2 = Seq(
      Row("Canada", 1),
      Row("China", 50),
      Row("Singapore", 22),
      Row("US", 205)
    )
    currentCP = spark.read.format("hudi")
      .load(countryToPopulationTblPath)
      .select("country", "population")
      .sort("country")
    checkAnswer(currentCP, countryRes2)

    stream1.stop()
    stream2.stop()
  }
}
