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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import org.apache.hudi.HoodieBaseRelation.{BaseFileReader, generateUnsafeProjection}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.LogIteratorUtils._
import org.apache.hudi.internal.schema.InternalSchema

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.io.Closeable
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], iterates over all of the records stored in
 * Delta Log files (represented as [[InternalRow]]s)
 */
class LogFileIterator(split: HoodieMergeOnReadFileSplit,
                      tableSchema: HoodieTableSchema,
                      requiredSchema: HoodieTableSchema,
                      tableState: HoodieTableState,
                      config: Configuration)
  extends Iterator[InternalRow] with Closeable with AvroDeserializerSupport {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  protected val payloadProps = tableState.preCombineFieldOpt
    .map { preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    }.getOrElse(new Properties())

  protected override val avroSchema: Schema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
  protected override val structTypeSchema: StructType = requiredSchema.structTypeSchema

  protected val logFileReaderAvroSchema: Schema = new Schema.Parser().parse(tableSchema.avroSchemaStr)

  protected var recordToLoad: InternalRow = _

  private val requiredSchemaSafeAvroProjection = SafeAvroProjection.create(logFileReaderAvroSchema, avroSchema)

  // TODO: now logScanner with internalSchema support column project, we may no need projectAvroUnsafe
  private var logScanner = {
    val internalSchema = tableSchema.internalSchema.getOrElse(InternalSchema.getEmptyInternalSchema)
    scanLog(split.logFiles, getPartitionPath(split), logFileReaderAvroSchema, tableState,
      maxCompactionMemoryInBytes, config, internalSchema)
  }

  private val logRecords = logScanner.getRecords.asScala

  def logRecordsIterator(): Iterator[(String, HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])] = {
    logRecords.iterator.asInstanceOf[Iterator[(String, HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])]]
  }

  // NOTE: This have to stay lazy to make sure it's initialized only at the point where it's
  //       going to be used, since we modify `logRecords` before that and therefore can't do it any earlier
  protected lazy val genericRecordsIterator: Iterator[Option[GenericRecord]] =
  logRecords.iterator.map {
    case (_, record) =>
      toScalaOption(record.getData.getInsertValue(logFileReaderAvroSchema, payloadProps))
        .map(_.asInstanceOf[GenericRecord])
  }

  protected def removeLogRecord(key: String): Option[HoodieRecord[_ <: HoodieRecordPayload[_]]] =
    logRecords.remove(key)

  override def hasNext: Boolean = hasNextInternal

  // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
  //       that recursion is unfolded into a loop to avoid stack overflows while
  //       handling records
  @tailrec private def hasNextInternal: Boolean = {
    genericRecordsIterator.hasNext && {
      val avroRecordOpt = genericRecordsIterator.next()
      if (avroRecordOpt.isEmpty) {
        // Record has been deleted, skipping
        this.hasNextInternal
      } else {
        val projectedAvroRecord = requiredSchemaSafeAvroProjection(avroRecordOpt.get)
        recordToLoad = deserialize(projectedAvroRecord)
        true
      }
    }
  }

  override final def next(): InternalRow = recordToLoad

  override def close(): Unit =
    if (logScanner != null) {
      try {
        logScanner.close()
      } finally {
        logScanner = null
      }
    }
}

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
 * Base file as well as all of the Delta Log files simply returning concatenation of these streams, while not
 * performing any combination/merging of the records w/ the same primary keys (ie producing duplicates potentially)
 */
private class SkipMergeIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileReader: BaseFileReader,
                                dataSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                tableState: HoodieTableState,
                                config: Configuration)
  extends LogFileIterator(split, dataSchema, requiredSchema, tableState, config) {

  private val requiredSchemaUnsafeProjection = generateUnsafeProjection(baseFileReader.schema, structTypeSchema)

  private val baseFileIterator = baseFileReader(split.dataFile.get)

  override def hasNext: Boolean = {
    if (baseFileIterator.hasNext) {
      // No merge is required, simply load current row and project into required schema
      recordToLoad = requiredSchemaUnsafeProjection(baseFileIterator.next())
      true
    } else {
      super[LogFileIterator].hasNext
    }
  }
}

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
 * a) Base file and all of the b) Delta Log files combining records with the same primary key from both of these
 * streams
 */
class RecordMergingFileIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileReader: BaseFileReader,
                                dataSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                tableState: HoodieTableState,
                                config: Configuration)
  extends LogFileIterator(split, dataSchema, requiredSchema, tableState, config) {

  // NOTE: Record-merging iterator supports 2 modes of operation merging records bearing either
  //        - Full table's schema
  //        - Projected schema
  //       As such, no particular schema could be assumed, and therefore we rely on the caller
  //       to correspondingly set the scheme of the expected output of base-file reader
  private val baseFileReaderAvroSchema = sparkAdapter.getAvroSchemaConverters.toAvroType(baseFileReader.schema, nullable = false, "record")

  private val serializer = sparkAdapter.createAvroSerializer(baseFileReader.schema, baseFileReaderAvroSchema, nullable = false)

  private val reusableRecordBuilder: GenericRecordBuilder = new GenericRecordBuilder(avroSchema)

  private val recordKeyOrdinal = baseFileReader.schema.fieldIndex(tableState.recordKeyField)

  private val requiredSchemaUnsafeProjection = generateUnsafeProjection(baseFileReader.schema, structTypeSchema)

  private val baseFileIterator = baseFileReader(split.dataFile.get)

  override def hasNext: Boolean = hasNextInternal

  // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
  //       that recursion is unfolded into a loop to avoid stack overflows while
  //       handling records
  @tailrec private def hasNextInternal: Boolean = {
    if (baseFileIterator.hasNext) {
      val curRow = baseFileIterator.next()
      val curKey = curRow.getString(recordKeyOrdinal)
      val updatedRecordOpt = removeLogRecord(curKey)
      if (updatedRecordOpt.isEmpty) {
        // No merge is required, simply load current row and project into required schema
        recordToLoad = requiredSchemaUnsafeProjection(curRow)
        true
      } else {
        val mergedAvroRecordOpt = merge(serialize(curRow), updatedRecordOpt.get)
        if (mergedAvroRecordOpt.isEmpty) {
          // Record has been deleted, skipping
          this.hasNextInternal
        } else {
          val projectedAvroRecord = projectAvroUnsafe(mergedAvroRecordOpt.get.asInstanceOf[GenericRecord],
            avroSchema, reusableRecordBuilder)
          recordToLoad = deserialize(projectedAvroRecord)
          true
        }
      }
    } else {
      super[LogFileIterator].hasNext
    }
  }

  private def serialize(curRowRecord: InternalRow): GenericRecord =
    serializer.serialize(curRowRecord).asInstanceOf[GenericRecord]

  private def merge(curAvroRecord: GenericRecord, newRecord: HoodieRecord[_ <: HoodieRecordPayload[_]]): Option[IndexedRecord] = {
    // NOTE: We have to pass in Avro Schema used to read from Delta Log file since we invoke combining API
    //       on the record from the Delta Log
    toScalaOption(newRecord.getData.combineAndGetUpdateValue(curAvroRecord, logFileReaderAvroSchema, payloadProps))
  }
}
