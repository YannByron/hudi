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

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodieMergeOnReadFileSplit, HoodieTableSchema, HoodieTableState, HoodieUnsafeRDD, LogFileIterator, LogIteratorUtils, RecordMergingFileIterator, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils._
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.cdc.CDCFileTypeEnum._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile, HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.cdc.CDCUtils
import org.apache.hudi.common.table.log.CDCLogRecordReader
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodiePayloadConfig

import org.apache.spark.{Partition, SerializableWritable, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util.Properties
import java.util.stream.Collectors

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The split that will be processed by spark task.
 */
case class HoodieCDCFileGroupSplit(
    commitToChanges: Array[(HoodieInstant, ChangeFileForSingleFileGroupAndCommit)]
)

/**
 * The Spark [[Partition]]'s implementation.
 */
case class HoodieCDCFileGroupPartition(
    index: Int,
    split: HoodieCDCFileGroupSplit
) extends Partition

class HoodieCDCRDD(
    spark: SparkSession,
    metaClient: HoodieTableMetaClient,
    cdcSupplementalLogging: Boolean,
    parquetReader: PartitionedFile => Iterator[InternalRow],
    originTableSchema: HoodieTableSchema,
    cdcSchema: StructType,
    requiredCdcSchema: StructType,
    changes: Array[HoodieCDCFileGroupSplit])
  extends RDD[InternalRow](spark.sparkContext, Nil) with HoodieUnsafeRDD {

  @transient private val hadoopConf = spark.sparkContext.hadoopConfiguration

  private val confBroadcast = spark.sparkContext.broadcast(new SerializableWritable(hadoopConf))

  private val props = HoodieFileIndex.getConfigProperties(spark, Map.empty)

  protected val payloadProps: Properties = Option(metaClient.getTableConfig.getPreCombineField)
    .map { preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    }.getOrElse(new Properties())

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val cdcPartition = split.asInstanceOf[HoodieCDCFileGroupPartition]
    new CDCFileGroupIterator(cdcPartition.split, metaClient, cdcSupplementalLogging)
  }

  override protected def getPartitions: Array[Partition] = {
    changes.zipWithIndex.map{ case (split, index) =>
      HoodieCDCFileGroupPartition(index, split)
    }.toArray
  }

  private class CDCFileGroupIterator(
      split: HoodieCDCFileGroupSplit,
      metaClient: HoodieTableMetaClient,
      cdcSupplementalLogging: Boolean
    ) extends Iterator[InternalRow] with SparkAdapterSupport with AvroDeserializerSupport with Closeable {

    private val fs = metaClient.getFs.getFileSystem

    private val conf = new Configuration(confBroadcast.value.value)

    private val basePath = metaClient.getBasePathV2

    private val recordKeyField: String = if (metaClient.getTableConfig.populateMetaFields()) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = metaClient.getTableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }

    private val preCombineFieldOpt: Option[String] = Option(metaClient.getTableConfig.getPreCombineField)

    private val tableState = {
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(props)
        .build();
      HoodieTableState(
        pathToString(basePath),
        split.commitToChanges.map(_._1.getTimestamp).max,
        recordKeyField,
        preCombineFieldOpt,
        usesVirtualKeys = false,
        metaClient.getTableConfig.getPayloadClass,
        metadataConfig
      )
    }

    private lazy val mapper: ObjectMapper = {
      val _mapper = new ObjectMapper
      _mapper.setSerializationInclusion(Include.NON_ABSENT)
      _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      _mapper.registerModule(DefaultScalaModule)
      _mapper
    }

    protected override val avroSchema: Schema = new Schema.Parser().parse(originTableSchema.avroSchemaStr)

    protected override val structTypeSchema: StructType = originTableSchema.structTypeSchema

    private val requiredIndexes: List[Int] = 0.until(structTypeSchema.length).toList

    private val serializer = sparkAdapter.createAvroSerializer(originTableSchema.structTypeSchema,
      avroSchema, resolveAvroSchemaNullability(avroSchema))

    /**
     * the deserializer used to convert the CDC GenericRecord to Spark InternalRow.
     */
    private val cdcRecordDeserializer: HoodieAvroDeserializer =
      sparkAdapter.createAvroDeserializer(CDCUtils.CDC_SCHEMA, CDCRelation.cdcSchema())

    private val projection: UnsafeProjection = generateUnsafeProjection(cdcSchema, requiredCdcSchema)

    // iterator on cdc file
    private val cdcFileIter = split.commitToChanges.sortBy(_._1).iterator

    // The instant that is currently being processed
    private var currentInstant: HoodieInstant = _

    // The cdc file that is currently being processed
    private var currentCdcFile: ChangeFileForSingleFileGroupAndCommit = _

    /**
     * two cases will use this to iterator the records:
     * 1) extract the change data from the base file directly, including 'PureAddFile' and 'PureRemoveFile'.
     * 2) when the type of cdc file is 'FileGroupReplaced',
     *    use this to trace the records that are converted from the 'dependentRecords'
     */
    private var recordIter: Iterator[InternalRow] = Iterator.empty

    /**
     * Only one case where it will be used is that extract the change data from log files for mor table.
     * At the time, 'logRecordIter' will work with 'dependentRecords' that keep all the records of the previous file slice.
     */
    private var logRecordIter: Iterator[(String, HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])] = Iterator.empty

    /**
     * Only one case where it will be used is that extract the change data from cdc log files.
     */
    private var cdcRecordReader: CDCLogRecordReader = _

    /**
     * the next record need to be returned when call next().
     */
    protected var recordToLoad: InternalRow = _

    /**
     * The list of files to which 'dependentRecords' belong.
     * Use it to determine if 'dependentRecords' contains all the required data that extract
     * the change data from the current cdc file.
     */
    private val dependentFiles: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty

    /**
     * For the file group is replaced or this is a mor log file, we need to merge with the other
     * data to get the correct change data. This is used to keep the dependent data.
     */
    private var dependentRecords: mutable.Map[String, GenericRecord] = mutable.Map.empty

    private def needLoadNextFile: Boolean = {
      !recordIter.hasNext &&
        !logRecordIter.hasNext &&
        (cdcRecordReader == null || !cdcRecordReader.hasNext)
    }

    @tailrec final def hasNextInternal: Boolean = {
      if (needLoadNextFile) {
        loadCdcFile()
      }
      if (currentCdcFile == null) {
        false
      } else {
        currentCdcFile.cdcFileType match {
          case PureAddFile | PureRemoveFile =>
            if (recordIter.hasNext && loadNext()) {
              true
            } else {
              hasNextInternal
            }
          case MorLogFile =>
            if (logRecordIter.hasNext && loadNext()) {
              true
            } else {
              hasNextInternal
            }
          case CDCLogFile =>
            if (cdcRecordReader.hasNext && loadNext()) {
              true
            } else {
              hasNextInternal
            }
          case FileGroupReplaced =>
            if (recordIter.hasNext && loadNext()) {
              true
            } else {
              hasNextInternal
            }
        }
      }
    }

    override def hasNext: Boolean = hasNextInternal

    override final def next(): InternalRow = {
      projection(recordToLoad)
    }

    def loadNext(): Boolean = {
      var loaded = false
      currentCdcFile.cdcFileType match {
        case PureAddFile =>
          val originRecord = recordIter.next()
          recordToLoad.update(3, convertRowToJsonString(originRecord))
          loaded = true
        case PureRemoveFile =>
          val originRecord = recordIter.next()
          recordToLoad.update(2, convertRowToJsonString(originRecord))
          loaded = true
        case MorLogFile =>
          val (key, logRecord) = logRecordIter.next()
          val indexedRecord = getInsertValue(logRecord)
          if (indexedRecord.isEmpty) {
            val existingRecordOpt = dependentRecords.remove(key)
            if (existingRecordOpt.isEmpty) {
              logWarning("can not get any record that have the same key with the deleting logRecord.")
            } else {
              // deleted record
              recordToLoad.update(0, CDCRelation.CDC_OPERATION_DELETE)
              recordToLoad.update(2, convertRowToJsonString(deserialize(existingRecordOpt.get)))
              recordToLoad.update(3, null)
              loaded = true
            }
          } else {
            val existingRecordOpt = dependentRecords.get(key)
            if (existingRecordOpt.isEmpty) {
              // inserted record
              val insertedRecord = convertIndexedRecordToRow(indexedRecord.get)
              recordToLoad.update(0, CDCRelation.CDC_OPERATION_INSERT)
              recordToLoad.update(2, null)
              recordToLoad.update(3, convertRowToJsonString(insertedRecord))
              // insert into records
              dependentRecords(key) = serialize(insertedRecord)
              loaded = true
            } else {
              // updated record
              val existingRecord = existingRecordOpt.get
              val merged = merge(existingRecord, logRecord)
              val mergeRow = convertIndexedRecordToRow(merged)
              val existingRow = deserialize(existingRecord)
              if (mergeRow != existingRow) {
                recordToLoad.update(0, CDCRelation.CDC_OPERATION_UPDATE)
                recordToLoad.update(2, convertRowToJsonString(existingRow))
                recordToLoad.update(3, convertRowToJsonString(mergeRow))
                // insert into records
                dependentRecords(key) = serialize(mergeRow)
                loaded = true
              }
            }
          }
        case CDCLogFile =>
          val record = cdcRecordReader.next().asInstanceOf[GenericRecord]
          recordToLoad = cdcRecordDeserializer.deserialize(record).get.asInstanceOf[InternalRow]
          loaded = true
        case FileGroupReplaced =>
          val originRecord = recordIter.next()
          recordToLoad.update(2, convertRowToJsonString(originRecord))
          loaded = true
      }
      loaded
    }

    private def loadCdcFile(): Unit = {
      // reset all the iterator or reader first.
      recordIter = Iterator.empty
      logRecordIter = Iterator.empty
      if (cdcRecordReader != null) {
        cdcRecordReader.close()
        cdcRecordReader = null
      }

      if (cdcFileIter.hasNext) {
        val pair = cdcFileIter.next()
        currentInstant = pair._1
        currentCdcFile = pair._2
        currentCdcFile.cdcFileType match {
          case PureAddFile | PureRemoveFile =>
            val absCDCPath = new Path(basePath, currentCdcFile.cdcFile)
            val fileStatus = fs.getFileStatus(absCDCPath)
            val pf = PartitionedFile(InternalRow.empty, absCDCPath.toUri.toString, 0, fileStatus.getLen)
            recordIter = parquetReader(pf)
          case MorLogFile =>
            currentCdcFile.dependentFileSlice.foreach(loadDependentFileSliceIfNeeded)
            val absLogPath = new Path(basePath, currentCdcFile.cdcFile)
            val morSplit = HoodieMergeOnReadFileSplit(None, List(new HoodieLogFile(fs.getFileStatus(absLogPath))))
            val logFileIterator = new LogFileIterator(morSplit, originTableSchema, originTableSchema, tableState, conf)
            logRecordIter = logFileIterator.genericRecordIterWithKey()
          case CDCLogFile =>
            val absCDCPath = new Path(basePath, currentCdcFile.cdcFile)
            cdcRecordReader = new CDCLogRecordReader(fs, absCDCPath)
          case FileGroupReplaced =>
            currentCdcFile.dependentFileSlice.foreach(loadDependentFileSliceIfNeeded)
            recordIter = dependentRecords.values.map { record =>
              deserialize(record)
            }.iterator
            dependentRecords = mutable.Map.empty
        }
        resetRecordFormat()
      } else {
        currentInstant = null
        currentCdcFile = null
      }
    }

    /**
     * Initialize the partial fields of the data to be returned in advance to speed up.
     */
    private def resetRecordFormat(): Unit = {
      recordToLoad = currentCdcFile.cdcFileType match {
        case PureAddFile =>
          InternalRow.fromSeq(Array(
            CDCRelation.CDC_OPERATION_INSERT, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case PureRemoveFile =>
          InternalRow.fromSeq(Array(
            CDCRelation.CDC_OPERATION_DELETE, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case MorLogFile =>
          InternalRow.fromSeq(Array(
            null, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case CDCLogFile =>
          InternalRow.fromSeq(Array(
            null, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
        case FileGroupReplaced =>
          InternalRow.fromSeq(Array(
            CDCRelation.CDC_OPERATION_DELETE, convertToUTF8String(currentInstant.getTimestamp),
            null, null))
      }
    }

    /**
     *
     */
    private def loadDependentFileSliceIfNeeded(fileSlice: FileSlice): Unit = {
      val baseFileLoaded = toScalaOption(fileSlice.getBaseFile).exists { baseFile =>
        dependentFiles.contains(baseFile.getPath)
      }
      val allLogFilesLoaded = fileSlice.getLogFiles.collect(Collectors.toList[HoodieLogFile]())
        .asScala.forall { logfile =>
        dependentFiles.contains(pathToString(logfile.getPath))
      }
      if (!baseFileLoaded || !allLogFilesLoaded) {
        loadDependentFileSlice(fileSlice)
        toScalaOption(fileSlice.getBaseFile).foreach { baseFile =>
          dependentFiles.append(baseFile.getPath)
        }
        fileSlice.getLogFiles.collect(Collectors.toList[HoodieLogFile]())
          .asScala.foreach { logfile =>
          dependentFiles.append(pathToString(logfile.getPath))
        }
      }
    }

    /**
     *
     */
    private def loadDependentFileSlice(fileSlice: FileSlice): Unit = {
      // clear up the dependentRecords
      dependentRecords = mutable.Map.empty

      // load fileSlice to dependentRecords
      val baseFileStatus = fs.getFileStatus(new Path(fileSlice.getBaseFile.get().getPath))
      val basePartitionedFile = PartitionedFile(
        InternalRow.empty,
        pathToString(baseFileStatus.getPath),
        0,
        baseFileStatus.getLen
      )
      val logFiles = fileSlice.getLogFiles
        .sorted(HoodieLogFile.getLogFileComparator)
        .collect(Collectors.toList[HoodieLogFile])
        .asScala.toList

      val iter = if (logFiles.isEmpty) {
        // no log files, just load the base parquet file
        parquetReader(basePartitionedFile)
      } else {
        // use [[RecordMergingFileIterator]] to load both the base file and log files
        val morSplit = HoodieMergeOnReadFileSplit(Some(basePartitionedFile), logFiles)
        val baseIter = parquetReader(basePartitionedFile)
        new RecordMergingFileIterator(
          morSplit,
          baseIter,
          originTableSchema,
          originTableSchema,
          originTableSchema,
          tableState,
          conf)
      }
      iter.foreach { row =>
        val key = row.getString(2)
        dependentRecords.put(key, serialize(row))
      }
    }

    /**
     * convert InternalRow to json string.
     */
    private def convertRowToJsonString(record: InternalRow): UTF8String = {
      val map = scala.collection.mutable.Map.empty[String, Any]
      originTableSchema.structTypeSchema.zipWithIndex.foreach {
        case (field, idx) =>
          if (field.dataType.isInstanceOf[StringType]) {
            map(field.name) = record.getString(idx)
          } else {
            map(field.name) = record.get(idx, field.dataType)
          }
      }
      convertToUTF8String(mapper.writeValueAsString(map))
    }

    /**
     * The data of string type is stored in InternalRow using UTF8String type.
     */
    private def convertToUTF8String(str: String): UTF8String = {
      UTF8String.fromString(str)
    }

    private def resolveAvroSchemaNullability(schema: Schema): Boolean = {
      AvroConversionUtils.resolveAvroTypeNullability(schema) match {
        case (nullable, _) => nullable
      }
    }

    private def pathToString(p: Path): String = {
      p.toUri.toString
    }

    private def serialize(curRowRecord: InternalRow): GenericRecord = {
      serializer.serialize(curRowRecord).asInstanceOf[GenericRecord]
    }

    private def getInsertValue(
        record: HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])
    : Option[IndexedRecord] = {
      toScalaOption(record.getData.getInsertValue(avroSchema, payloadProps))
    }

    private def convertIndexedRecordToRow(record: IndexedRecord): InternalRow = {
      deserialize(
        LogIteratorUtils.projectAvroUnsafe(record, avroSchema, requiredIndexes, new GenericRecordBuilder(avroSchema))
      )
    }

    private def merge(curAvroRecord: GenericRecord, newRecord: HoodieRecord[_ <: HoodieRecordPayload[_]]): IndexedRecord = {
      newRecord.getData.combineAndGetUpdateValue(curAvroRecord, avroSchema, payloadProps).get()
    }

    private def generateUnsafeProjection(from: StructType, to: StructType): UnsafeProjection =
      sparkAdapter.createCatalystExpressionUtils().generateUnsafeProjection(from, to)

    override def close(): Unit = {}
  }
}
