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

package org.apache.hudi.cdc

import org.apache.avro.Schema

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.hudi.cdc.CDCFileTypeEnum._
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.HoodieDataSourceHelper
import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.model.HoodieFileGroupId
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.model.HoodieWriteStat
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class CDCRelation(
    override val sqlContext: SQLContext,
    metaClient: HoodieTableMetaClient,
    startInstant: String,
    endInstant: String,
    options: Map[String, String]
) extends BaseRelation with PrunedFilteredScan with Logging {

  sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")

  val spark: SparkSession = sqlContext.sparkSession

  val conf: Configuration = spark.sparkContext.hadoopConfiguration

  val jsc: JavaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext)

  val fs: FileSystem = metaClient.getFs.getFileSystem

  val basePath: Path = metaClient.getBasePathV2

  val tableConfig: HoodieTableConfig = metaClient.getTableConfig

  val (tableAvroSchema: Schema, internalSchema: InternalSchema) = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val avroSchema = Try(schemaUtil.getTableAvroSchema) match {
      case Success(schema) => schema
      case Failure(e) =>
        throw new IllegalArgumentException("Failed to fetch schema from the table", e)
    }
    // try to find internalSchema
    val internalSchemaFromMeta = try {
      schemaUtil.getTableInternalSchemaFromCommitMetadata.orElse(InternalSchema.getEmptyInternalSchema)
    } catch {
      case _: Exception => InternalSchema.getEmptyInternalSchema
    }
    (avroSchema, internalSchemaFromMeta)
  }

  val tableStructSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)

  val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  val commits: Map[HoodieInstant, HoodieCommitMetadata] =
    CDCRelation.getCompletedInstantInSpecifiedRange(metaClient, startInstant, endInstant)

  val touchedPartition: Set[String] = commits.flatMap { case (_, commitMetadata) =>
    commitMetadata.getPartitionToWriteStats.keySet().asScala
  }.toSet

  val changeFilesPerFileGroupAndCommit: Map[HoodieFileGroupId, HoodieCDCFileGroupSplit] = {
    // At the granularity of a file group, keep the mapping between each commit and
    // changes to this file group. Here the writeStat of this file group and this commit
    // is also kept to extract the base/log files if needed.
    val fgToCommitChanges = mutable.Map.empty[HoodieFileGroupId,
      mutable.Map[HoodieInstant, (HoodieWriteStat, ChangeFileForSingleFileGroup)]]

    commits.foreach {
      case (instant, commitMetadata)
          if CDCRelation.maybeChangeData(commitMetadata.getOperationType) =>
        commitMetadata.getPartitionToWriteStats.asScala.foreach {
          case (partition, hoodieWriteStats) =>
            hoodieWriteStats.asScala.foreach { writeStat =>
              val fileGroupId = new HoodieFileGroupId(partition, writeStat.getFileId)
              // Identify the CDC source involved in this commit and
              // determine its type for subsequent loading using different methods.
              val (cdcFileType, cdcFile) = parseWriteStat(commitMetadata.getOperationType, writeStat)
              val realCdcFile = if (cdcFileType == PureRemoveFile) {
                // This represents the operation is a delete or drop-partition operation,
                // wherein no records have been writen out a new file.
                // So, we find the previous file that this operation delete from, and treat each of
                // records as a deleted one.
                getPrevCommitFileWithSameFG(fileGroupId, writeStat.getPrevCommit)
              } else {
                cdcFile
              }
              val changeFile = ChangeFileForSingleFileGroup(cdcFileType, realCdcFile)
              if (fgToCommitChanges.contains(fileGroupId)) {
                fgToCommitChanges(fileGroupId)(instant) = (writeStat, changeFile)
              } else {
                fgToCommitChanges.put(fileGroupId, mutable.Map(instant -> (writeStat, changeFile)))
              }
            }
        }
    }
    fgToCommitChanges.map { case (fgId, instantToWriteStatAndChanges) =>
      val changesArr = instantToWriteStatAndChanges.map { case (instant, (writeStat, changes)) =>
        (instant, changes)
      }.toArray.sortBy(_._1)
      val commitTimeAndFileSlice = changesArr.find(_._2.cdcFileType == MorLogFile).map {
        case (instant, changeFile) =>
          // if the cdc path is a log file, we need to find the base file and its preceding
          // log files which will be used when load this log file.
          // And since the commit scope read by CDC is continuous and the change
          // of the same file group is also continuous, we only need to find the preceding
          // file slice of the earliest commit required.
          val fileSlice = buildDependentFileSliceForLogFile(
            fgId, instant, changeFile.cdcFile, instantToWriteStatAndChanges(instant)._1)
          (instant.getTimestamp, fileSlice)
      }
      (fgId, HoodieCDCFileGroupSplit(fgId, changesArr, commitTimeAndFileSlice))
    }.toMap
  }

  override final def needConversion: Boolean = false

  override def schema: StructType = CDCRelation.cdcSchema()

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val tableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchema.toString)
    val parquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = spark,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = filters,
      options = options,
      hadoopConf = conf
    )
    new HoodieCDCRDD(
      spark,
      metaClient,
      parquetReader,
      tableSchema,
      changeFilesPerFileGroupAndCommit.values.toArray
    ).asInstanceOf[RDD[Row]]
  }

  private def getPrevCommitFileWithSameFG(fgId: HoodieFileGroupId, commitTime: String): String = {
    val absPartitionPath = if (StringUtils.isNullOrEmpty(fgId.getPartitionPath)) {
      basePath
    } else {
      new Path(basePath, fgId.getPartitionPath)
    }
    FSUtils.getBaseFile(fs, absPartitionPath, fgId.getFileId, commitTime).getPath.toUri.toString
  }

  private def parseWriteStat(operation: WriteOperationType, writeStat: HoodieWriteStat): (CDCFileType, String) = {
    if (StringUtils.isNullOrEmpty(writeStat.getCDCPath)) {
      val path = writeStat.getPath
      if (path.endsWith(HoodieFileFormat.PARQUET.getFileExtension)) {
        // this is a base file
        if (operation == WriteOperationType.DELETE
          && writeStat.getNumWrites == 0L && writeStat.getNumDeletes != 0) {
          // all the records in this file group have been deleted
          (PureRemoveFile, null)
        } else if (writeStat.getNumUpdateWrites == 0L && writeStat.getNumDeletes == 0
          && writeStat.getNumWrites == writeStat.getNumInserts) {
          // all the records in this file are new.
          (PureAddFile, path)
        } else {
          throw new HoodieException("There should be a cdc log file.")
        }
      } else {
        // this is a log file
        (MorLogFile, path)
      }
    } else {
      // this is a cdc log
      (CDCLogFile, writeStat.getCDCPath)
    }
  }

  private def buildDependentFileSliceForLogFile(
      fgId: HoodieFileGroupId,
      instant: HoodieInstant,
      currentLogFile: String,
      writeStat: HoodieWriteStat): FileSlice = {
    val baseCommitTime = FSUtils.getCommitTime(writeStat.getPath)
    val partitionPath = if (StringUtils.isNullOrEmpty(fgId.getPartitionPath)) {
      basePath
    } else {
      new Path(basePath, fgId.getPartitionPath)
    }
    val baseFileStatus = FSUtils.getBaseFile(fs, partitionPath, fgId.getFileId, baseCommitTime)
    val baseFile = new HoodieBaseFile(baseFileStatus)

    val activeTimeLine = metaClient.getActiveTimeline
    val logFilesStr = HoodieCommitMetadata.getFileSliceForDeltaCommit(activeTimeLine.getInstantDetails(instant).get())
      .get(fgId).getRight.asScala
      .filter(_ != currentLogFile)
      .map(new Path(partitionPath, _)).toArray
    val logFiles = fs.listStatus(logFilesStr).map(new HoodieLogFile(_))
    new FileSlice(fgId, baseCommitTime, baseFile, logFiles)
  }
}

object CDCRelation {

  /* the suffix of cdc log file */
  val CDC_SUFFIX = ".cdc"

  /* the `op` column when enable cdc */
  val CDC_OPERATION_TYPE = "op"

  /* the timestamp when each of record is changed */
  val CDC_COMMIT_TIMESTAMP = "ts_ms"

  /* the pre image before one record is changed */
  val CDC_BEFORE_IMAGE = "before"

  /* the post image after one record is changed */
  val CDC_AFTER_IMAGE = "after"

  val CDC_OPERATION_DELETE: UTF8String = UTF8String.fromString("d")
  val CDC_OPERATION_INSERT: UTF8String = UTF8String.fromString("i")
  val CDC_OPERATION_UPDATE: UTF8String = UTF8String.fromString("u")

  def cdcSchema(): StructType = {
    StructType(
      Seq(
        StructField(CDC_OPERATION_TYPE, StringType),
        StructField(CDC_COMMIT_TIMESTAMP, StringType),
        StructField(CDC_BEFORE_IMAGE, StringType),
        StructField(CDC_AFTER_IMAGE, StringType)
      )
    )
  }

  def getCDCRelation(
      sqlContext: SQLContext,
      metaClient: HoodieTableMetaClient,
      options: Map[String, String]): CDCRelation = {

    if (!isCDCTable(metaClient)) {
      throw new IllegalArgumentException(s"It isn't a CDC hudi table on ${metaClient.getBasePathV2.toString}")
    }

    val startingInstant = options.getOrElse(DataSourceReadOptions.BEGIN_INSTANTTIME.key(),
      throw new HoodieException("CDC Query should provide the valid start version or timestamp")
    )
    val endingInstant = options.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key(),
      getTimestampOfLatestInstant(metaClient)
    )
    if (startingInstant > endingInstant) {
      throw new HoodieException(s"This is not a valid range between $startingInstant and $endingInstant")
    }

    new CDCRelation(sqlContext, metaClient, startingInstant, endingInstant, options)
  }

  def isCDCTable(metaClient: HoodieTableMetaClient): Boolean = {
    metaClient.getTableConfig.isEnabledCDF
  }

  def getTimestampOfLatestInstant(metaClient: HoodieTableMetaClient): String = {
    val latestInstant = metaClient.getActiveTimeline.lastInstant()
    if (latestInstant.isPresent) {
      latestInstant.get().getTimestamp
    } else {
      throw new HoodieException("No valid instant in Active Timeline.")
    }
  }

  def getCompletedInstantInSpecifiedRange(
      metaClient: HoodieTableMetaClient,
      startTs: String,
      endTs: String): Map[HoodieInstant, HoodieCommitMetadata] = {
    val activeTimeLine = metaClient.getActiveTimeline
    val instanstAndCommitMetadatas = activeTimeLine.getInstantsAsList.asScala
      .filter(s => s.isCompleted && HoodieTimeline.isInRange(s.getTimestamp, startTs, endTs))
      .map { instant =>
        val commitMetadata = HoodieCommitMetadata.fromBytes(
          activeTimeLine.getInstantDetails(instant).get(),
          classOf[HoodieCommitMetadata]
        )
        (instant, commitMetadata)
      }
    instanstAndCommitMetadatas.toMap
  }

  def maybeChangeData(operation: WriteOperationType): Boolean = {
    operation == WriteOperationType.INSERT ||
      operation == WriteOperationType.UPSERT ||
      operation == WriteOperationType.DELETE ||
      operation == WriteOperationType.BULK_INSERT ||
      operation == WriteOperationType.DELETE_PARTITION ||
      operation == WriteOperationType.INSERT_OVERWRITE ||
      operation == WriteOperationType.INSERT_OVERWRITE_TABLE ||
      operation == WriteOperationType.BOOTSTRAP
  }
}
