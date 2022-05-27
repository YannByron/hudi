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
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import LogIteratorUtils._
import org.apache.hudi.io.HoodieWriteHandle
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType

import java.io.Closeable
import java.util.Properties
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], iterates over all of the records stored in
 * Delta Log files (represented as [[InternalRow]]s)
 */
class LogFileIterator(
    split: HoodieMergeOnReadFileSplit,
    tableSchema: HoodieTableSchema,
    requiredSchema: HoodieTableSchema,
    tableState: HoodieTableState,
    config: Configuration,
    outputDeletedRecord: Boolean = false)
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

  protected val recordBuilder: GenericRecordBuilder = new GenericRecordBuilder(avroSchema)
  protected var recordToLoad: InternalRow = _

  // TODO validate whether we need to do UnsafeProjection
  protected val unsafeProjection: UnsafeProjection = UnsafeProjection.create(structTypeSchema)

  // NOTE: This maps _required_ schema fields onto the _full_ table schema, collecting their "ordinals"
  //       w/in the record payload. This is required, to project records read from the Delta Log file
  //       which always reads records in full schema (never projected, due to the fact that DL file might
  //       be stored in non-columnar formats like Avro, HFile, etc)
  private val requiredSchemaFieldOrdinals: List[Int] = collectFieldOrdinals(avroSchema, logFileReaderAvroSchema)

  // TODO: now logScanner with internalSchema support column project, we may no need projectAvroUnsafe
  private var logScanner =
    scanLog(split.logFiles, getPartitionPath(split), logFileReaderAvroSchema, tableState,
      maxCompactionMemoryInBytes, config, tableSchema.internalSchema)

  val logRecords = logScanner.getRecords.asScala

  def genericRecordIterWithKey(): Iterator[(String, HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]])] = logRecords.iterator
//    .map {
//    case (key, record) =>
//      val avroRecordOpt = record.getData.getInsertValue(logFileReaderAvroSchema, payloadProps)
//      if (avroRecordOpt.isPresent) {
//        (key, projectAvroUnsafe(
//          avroRecordOpt.get(), requiredAvroSchema, requiredSchemaFieldOrdinals, recordBuilder))
//      } else {
//        (key, HoodieWriteHandle.IGNORE_RECORD)
//      }
//  }

  // NOTE: This iterator iterates over already projected (in required schema) records
  // NOTE: This have to stay lazy to make sure it's initialized only at the point where it's
  //       going to be used, since we modify `logRecords` before that and therefore can't do it any earlier
  protected lazy val logRecordsIterator: Iterator[Option[GenericRecord]] =
  logRecords.iterator.map {
    case (_, record) =>
      val avroRecordOpt = toScalaOption(record.getData.getInsertValue(logFileReaderAvroSchema, payloadProps))
      avroRecordOpt.map {
        avroRecord => projectAvroUnsafe(avroRecord, avroSchema, requiredSchemaFieldOrdinals, recordBuilder)
      }
  }

  protected def removeLogRecord(key: String): Option[HoodieRecord[_ <: HoodieRecordPayload[_]]] =
    logRecords.remove(key)

  override def hasNext: Boolean = hasNextInternal

  // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
  //       that recursion is unfolded into a loop to avoid stack overflows while
  //       handling records
  @tailrec private def hasNextInternal: Boolean = {
    logRecordsIterator.hasNext && {
      val avroRecordOpt = logRecordsIterator.next()
      if (avroRecordOpt.isEmpty) {
        // Record has been deleted, skipping
        this.hasNextInternal
      } else {
        recordToLoad = unsafeProjection(deserialize(avroRecordOpt.get))
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
 * a) Base file and all of the b) Delta Log files combining records with the same primary key from both of these
 * streams
 */
class RecordMergingFileIterator(split: HoodieMergeOnReadFileSplit,
                                        baseFileIterator: Iterator[InternalRow],
                                        baseFileReaderSchema: HoodieTableSchema,
                                        tableSchema: HoodieTableSchema,
                                        requiredSchema: HoodieTableSchema,
                                        tableState: HoodieTableState,
                                        config: Configuration)
  extends LogFileIterator(split, tableSchema, requiredSchema, tableState, config) {

  // NOTE: Record-merging iterator supports 2 modes of operation merging records bearing either
  //        - Full table's schema
  //        - Projected schema
  //       As such, no particular schema could be assumed, and therefore we rely on the caller
  //       to correspondingly set the scheme of the expected output of base-file reader
  private val baseFileReaderAvroSchema = new Schema.Parser().parse(baseFileReaderSchema.avroSchemaStr)
  private val requiredSchemaFieldOrdinals: List[Int] = collectFieldOrdinals(avroSchema, baseFileReaderAvroSchema)

  private val serializer = sparkAdapter.createAvroSerializer(baseFileReaderSchema.structTypeSchema,
    baseFileReaderAvroSchema, resolveAvroSchemaNullability(baseFileReaderAvroSchema))

  private val recordKeyOrdinal = baseFileReaderSchema.structTypeSchema.fieldIndex(tableState.recordKeyField)

  override def hasNext: Boolean = hasNextInternal

  // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
  //       that recursion is unfolded into a loop to avoid stack overflows while
  //       handling records
  @tailrec private def hasNextInternal: Boolean = {
    if (baseFileIterator.hasNext) {
      val curRowRecord = baseFileIterator.next()
      val curKey = curRowRecord.getString(recordKeyOrdinal)
      val updatedRecordOpt = removeLogRecord(curKey)
      if (updatedRecordOpt.isEmpty) {
        // No merge needed, load current row with required projected schema
        recordToLoad = unsafeProjection(projectRowUnsafe(curRowRecord, requiredSchema.structTypeSchema, requiredSchemaFieldOrdinals))
        true
      } else {
        val mergedAvroRecordOpt = merge(serialize(curRowRecord), updatedRecordOpt.get)
        if (mergedAvroRecordOpt.isEmpty) {
          // Record has been deleted, skipping
          this.hasNextInternal
        } else {
          // NOTE: In occurrence of a merge we can't know the schema of the record being returned, b/c
          //       record from the Delta Log will bear (full) Table schema, while record from the Base file
          //       might already be read in projected one (as an optimization).
          //       As such we can't use more performant [[projectAvroUnsafe]], and instead have to fallback
          //       to [[projectAvro]]
          val projectedAvroRecord = projectAvro(mergedAvroRecordOpt.get, avroSchema, recordBuilder)
          recordToLoad = unsafeProjection(deserialize(projectedAvroRecord))
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
