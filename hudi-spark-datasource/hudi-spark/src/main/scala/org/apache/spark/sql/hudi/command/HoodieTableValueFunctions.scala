/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command

import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.{DataSourceReadOptions, DefaultSource, HoodieSparkUtils}
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class HoodieTableValueFunctions()

object HoodieTableValueFunctions {

  val TABLE_CHANGES_TVF_NAME = "table_changes"

  def resolveHoodieTvf(spark: SparkSession, tvf: UnresolvedTableValuedFunction): LogicalPlan = {
    val conf = spark.sessionState.conf
    val resolver = conf.resolver
    if (resolver(getTVFName(tvf), TABLE_CHANGES_TVF_NAME)) {
      val args = tvf.functionArgs

      if (args.length < 2 || args.length > 3) {
        throw new HoodieException("No enough args.")
      }

      val identifier = spark.sessionState.sqlParser.parseTableIdentifier(
        args.head.eval().toString)
      val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
      val cdcOptions = parseArgs(args)
      val dataSource = new DefaultSource
      val relation = dataSource.createRelation(spark.sqlContext, cdcOptions)
      new LogicalRelation(
        relation,
        CDCRelation.cdcSchema().toAttributes,
        Some(catalogTable),
        false
      )
    } else {
      tvf
    }
  }

  def parseArgs(args: Seq[Expression]): Map[String, String] = {
    val starting = args.head.eval().toString
    val ending = args.drop(1).headOption.map(_.eval().toString)

    Map(
      DataSourceReadOptions.QUERY_TYPE.key() -> DataSourceReadOptions.QUERY_TYPE_CDC_OPT_VAL,
      DataSourceReadOptions.BEGIN_INSTANTTIME.key()-> starting
    ) ++
      ending.map(DataSourceReadOptions.END_INSTANTTIME.key() -> _).toMap
  }

  def getTVFName(tvf: UnresolvedTableValuedFunction): String = {
    val fieldName = if (HoodieSparkUtils.gteqSpark3_2) {
      "name"
    } else {
      "functionName"
    }
    val clazz = tvf.getClass
    val field = clazz.getDeclaredField(fieldName)
    field.get(tvf).toString
  }
}
