package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.util.Try

object LogIteratorUtils {

  def scanLog(logFiles: List[HoodieLogFile],
              partitionPath: Path,
              logSchema: Schema,
              tableState: HoodieTableState,
              maxCompactionMemoryInBytes: Long,
              hadoopConf: Configuration, internalSchema: InternalSchema = InternalSchema.getEmptyInternalSchema): HoodieMergedLogRecordScanner = {
    val tablePath = tableState.tablePath
    val fs = FSUtils.getFs(tablePath, hadoopConf)

    if (HoodieTableMetadata.isMetadataTable(tablePath)) {
      val metadataConfig = tableState.metadataConfig
      val dataTableBasePath = getDataTableBasePathFromMetadataTable(tablePath)
      val metadataTable = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(hadoopConf), metadataConfig,
        dataTableBasePath,
        hadoopConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))

      // We have to force full-scan for the MT log record reader, to make sure
      // we can iterate over all of the partitions, since by default some of the partitions (Column Stats,
      // Bloom Filter) are in "point-lookup" mode
      val forceFullScan = true

      // NOTE: In case of Metadata Table partition path equates to partition name (since there's just one level
      //       of indirection among MT partitions)
      val relativePartitionPath = getRelativePartitionPath(new Path(tablePath), partitionPath)
      metadataTable.getLogRecordScanner(logFiles.asJava, relativePartitionPath, toJavaOption(Some(forceFullScan)))
        .getLeft
    } else {
      val logRecordScannerBuilder = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(tablePath)
        .withLogFilePaths(logFiles.map(logFile => logFile.getPath.toString).asJava)
        .withReaderSchema(logSchema)
        .withLatestInstantTime(tableState.latestCommitTimestamp)
        .withReadBlocksLazily(
          Try(hadoopConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED).toBoolean)
            .getOrElse(false))
        .withReverseReader(false)
        .withInternalSchema(internalSchema)
        .withBufferSize(
          hadoopConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
            HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withMaxMemorySizeInBytes(maxCompactionMemoryInBytes)
        .withSpillableMapBasePath(
          hadoopConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
            HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))

      if (logFiles.nonEmpty) {
        logRecordScannerBuilder.withPartition(
          getRelativePartitionPath(new Path(tableState.tablePath), logFiles.head.getPath.getParent))
      }

      logRecordScannerBuilder.build()
    }
  }

  /**
   * Projects provided instance of [[InternalRow]] into provided schema, assuming that the
   * the schema of the original row is strictly a superset of the given one
   */
  def projectRowUnsafe(row: InternalRow,
                       projectedSchema: StructType,
                       ordinals: Seq[Int]): InternalRow = {
    val projectedRow = new SpecificInternalRow(projectedSchema)
    var curIndex = 0
    projectedSchema.zip(ordinals).foreach { case (field, pos) =>
      val curField = if (row.isNullAt(pos)) {
        null
      } else {
        row.get(pos, field.dataType)
      }
      projectedRow.update(curIndex, curField)
      curIndex += 1
    }
    projectedRow
  }

  /**
   * Projects provided instance of [[IndexedRecord]] into provided schema, assuming that the
   * the schema of the original row is strictly a superset of the given one
   */
  def projectAvroUnsafe(record: IndexedRecord,
                        projectedSchema: Schema,
                        ordinals: List[Int],
                        recordBuilder: GenericRecordBuilder): GenericRecord = {
    val fields = projectedSchema.getFields.asScala
    checkState(fields.length == ordinals.length)
    fields.zip(ordinals).foreach {
      case (field, pos) => recordBuilder.set(field, record.get(pos))
    }
    recordBuilder.build()
  }

  /**
   * Projects provided instance of [[IndexedRecord]] into provided schema, assuming that the
   * the schema of the original row is strictly a superset of the given one
   *
   * This is a "safe" counterpart of [[projectAvroUnsafe]]: it does build mapping of the record's
   * schema into projected one itself (instead of expecting such mapping from the caller)
   */
  def projectAvro(record: IndexedRecord,
                  projectedSchema: Schema,
                  recordBuilder: GenericRecordBuilder): GenericRecord = {
    projectAvroUnsafe(record, projectedSchema, collectFieldOrdinals(projectedSchema, record.getSchema), recordBuilder)
  }

  /**
   * Maps [[projected]] [[Schema]] onto [[source]] one, collecting corresponding field ordinals w/in it, which
   * will be subsequently used by either [[projectRowUnsafe]] or [[projectAvroUnsafe()]] method
   *
   * @param projected target projected schema (which is a proper subset of [[source]] [[Schema]])
   * @param source    source schema of the record being projected
   * @return list of ordinals of corresponding fields of [[projected]] schema w/in [[source]] one
   */
  def collectFieldOrdinals(projected: Schema, source: Schema): List[Int] = {
    projected.getFields.asScala.map(f => source.getField(f.name()).pos()).toList
  }

  def getPartitionPath(split: HoodieMergeOnReadFileSplit): Path = {
    // Determine partition path as an immediate parent folder of either
    //    - The base file
    //    - Some log file
    split.dataFile.map(baseFile => new Path(baseFile.filePath))
      .getOrElse(split.logFiles.head.getPath)
      .getParent
  }

  def resolveAvroSchemaNullability(schema: Schema) = {
    AvroConversionUtils.resolveAvroTypeNullability(schema) match {
      case (nullable, _) => nullable
    }
  }
}
