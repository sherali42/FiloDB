package filodb.downsampler.chunk

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import kamon.Kamon
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.core.store.{ChunkSet, ChunkSetInfo}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.ExporterConstants.{METRIC, NAMESPACE, WORKSPACE}
import filodb.memory.BinaryRegionLarge
import filodb.memory.format.UnsafeUtils

case class ExportDSRowData(dsresolution: String,
                           partKey: Array[Byte],
                           chunkid: Long,
                           chunkinfo: Array[Byte],
                           workspace: String,
                           namespace: String,
                           metric: String,
                           starttime: Long,
                           endtime: Long,
                           numrows: Long,
                           schemaid: Int,
                           shard: Int,
                           date: Date,
                           day: Int,
                           month: Int,
                           year: Int,
                           hour: Int,
                           timestamp: Array[Byte],
                           value: Option[Array[Byte]],
                           avg: Option[Array[Byte]],
                           sum: Option[Array[Byte]],
                           count: Option[Array[Byte]],
                           min: Option[Array[Byte]],
                           max: Option[Array[Byte]],
                           h_buckets: Option[Array[Byte]],
                           tscount: Option[Array[Byte]])

/**
 * Exports a window of data to a bucket.
 */
case class BatchDownsampleExporter(val downsamplerSettings: DownsamplerSettings,
                                   val userStartTime: Long, val userEndTime: Long) {

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(downsamplerSettings.filodbConfig).get

  @transient lazy val numPartitionsExportPrepped = Kamon.counter("num-partitions-export-prepped").withoutTags()

  @transient lazy val numRowsExportPrepped = Kamon.counter("num-rows-export-prepped").withoutTags()

  val partitionByNames = Seq("workspace", "namespace", "year", "month", "day", "hour", "metric")
  val keyToRules = downsamplerSettings.exportKeyToRules
  val exportSchema = {
    // NOTE: ArrayBuffers are sometimes used instead of Seq's because otherwise
    //   ArrayIndexOutOfBoundsExceptions occur when Spark exports a batch.
    val fields = new mutable.ArrayBuffer[StructField](3 + downsamplerSettings.exportPathSpecPairs.size)
    fields.append(
      StructField("dsresolution", StringType),
      StructField("partkey", BinaryType),
      StructField("chunkid", LongType),
      StructField("chunkinfo", BinaryType),
      StructField("starttime", LongType),
      StructField("endtime", LongType),
      StructField("shard", IntegerType),
      StructField("numrows", LongType),
      StructField("timestamp", BinaryType),
      StructField("schemahash", IntegerType),
      StructField("value", BinaryType),
      StructField("avg", BinaryType),
      StructField("sum", BinaryType),
      StructField("count", BinaryType),
      StructField("min", BinaryType),
      StructField("max", BinaryType),
      StructField("h", BinaryType),
      StructField("tscount", BinaryType),
      StructField("workspace", StringType),
      StructField("namespace", StringType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType),
      StructField("hour", IntegerType),
      StructField("metric", StringType)
    )
    // append all partitioning columns as strings
    StructType(fields)
  }

  // One for each name; "template" because these still contain the {{}} notation to be replaced.
  // var partitionByValuesTemplate: Seq[String] = Nil
  // The indices of partitionByValuesTemplate that contain {{}} notation to be replaced.
  // var partitionByValuesIndicesWithTemplate: Set[Int] = Set.empty

  // Replace <<>> template notation with formatted dates.

  val date = new Date(userStartTime)
  val year = new SimpleDateFormat("yyyy").format(date).toInt
  val month = new SimpleDateFormat("MM").format(date).toInt
  val day = new SimpleDateFormat("dd").format(date).toInt
  val hour = new SimpleDateFormat("hh").format(date).toInt/6

  /**
   * Returns the Spark Rows to be exported.
   */
  // scalastyle:off method.length
  def getExportRows(chunksets: Map[FiniteDuration, Iterator[ChunkSet]]): Iterator[Row] = {

    DownsamplerContext.dsLogger.info(s"exporting chunks 115")
    DownsamplerContext.dsLogger.info(s"exporting chunks 116 $chunksets")
    @volatile var numChunks = 0
    chunksets.map { case (duration, chunks) =>
      DownsamplerContext.dsLogger.info(s"exporting chunks 119")
      val chunksToPersist = chunks.map { c =>
        numChunks += 1
        c.copy(listener = _ => {})
      }
      DownsamplerContext.dsLogger.info(s"exporting chunks size: $numChunks")
      chunksToPersist.map(chunkset => {
        var value: Option[Array[Byte]] = Option.empty
        var sum: Option[Array[Byte]]  = Option.empty
        var avg: Option[Array[Byte]]  = Option.empty
        var count: Option[Array[Byte]]  = Option.empty
        var min: Option[Array[Byte]]  = Option.empty
        var max: Option[Array[Byte]]  = Option.empty
        var hBuckets: Option[Array[Byte]]  = Option.empty
        var tscount: Option[Array[Byte]]  = Option.empty
        var timestamp: Option[Array[Byte]]  = Option.empty
        DownsamplerContext.dsLogger.info(s"exporting chunks check1")
        val partKeyBytes = BinaryRegionLarge.asNewByteArray(chunkset.partition)
        val chunkInfo = ChunkSetInfo.toBytes(chunkset.info)
        val schemaId = RecordSchema.schemaID(partKeyBytes, UnsafeUtils.arayOffset)
        val pairs = schemas(schemaId).partKeySchema.toStringPairs(partKeyBytes, UnsafeUtils.arayOffset).toMap
        val columns = schemas(schemaId).data.columns
        DownsamplerContext.dsLogger.debug(s"exporting pairs: $pairs")
        val metric = pairs(METRIC)
        val workspace = pairs(WORKSPACE)
        val namespace = pairs(NAMESPACE)
        columns.zipWithIndex.map { case (col, i) =>
          col.name match {
            case "timestamp" => timestamp = Some(byteBufToBytes(chunkset.chunks(i)))
            case "value" => value = Some(byteBufToBytes(chunkset.chunks(i)))
            case "avg" => avg = Some(byteBufToBytes(chunkset.chunks(i)))
            case "sum" => sum = Some(byteBufToBytes(chunkset.chunks(i)))
            case "min" => min = Some(byteBufToBytes(chunkset.chunks(i)))
            case "max" => max = Some(byteBufToBytes(chunkset.chunks(i)))
            case "count" => count = Some(byteBufToBytes(chunkset.chunks(i)))
            case "h" => hBuckets = Some(byteBufToBytes(chunkset.chunks(i)))
            case "tscount" => tscount = Some(byteBufToBytes(chunkset.chunks(i)))
          }
        }
        DownsamplerContext.dsLogger.info(s"exporting chunks check2")
        if (numChunks%100 == 0) {
          DownsamplerContext.dsLogger.info(s"timechunk:: ${timestamp.size}")
        }
        ExportDSRowData(duration.toString, partKeyBytes, chunkset.info.id, chunkInfo,
          workspace, namespace, metric = metric, starttime = chunkset.info.startTime, endtime = chunkset.info.endTime,
          numrows = chunkset.info.numRows, schemaid = schemaId, shard = 0, date = date, day = day,
          month = month, year = year, hour = hour, timestamp = timestamp.get, value = value, avg = avg, sum = sum,
          count = count, min = min, max = max, h_buckets = hBuckets, tscount = tscount)

      })
    }
      .flatMap { exportData =>
        numRowsExportPrepped.increment()
        exportData.map(exportDataToRow)
      }.toIterator
  }

  /**
   * Converts an ExportData to a Spark Row.
   * The result Row *must* match this.exportSchema.
   */
  private def exportDataToRow(exportData: ExportDSRowData): Row = {
    val dataSeq = new mutable.ArrayBuffer[Any](this.exportSchema.fields.length)
    dataSeq.append(
      exportData.dsresolution,
      exportData.partKey,
      exportData.chunkid,
      exportData.chunkinfo,
      exportData.starttime,
      exportData.endtime,
      exportData.shard,
      exportData.numrows,
      exportData.timestamp,
      exportData.schemaid,
      exportData.value.getOrElse(null),
      exportData.avg.getOrElse(null),
      exportData.sum.getOrElse(null),
      exportData.count.getOrElse(null),
      exportData.min.getOrElse(null),
      exportData.max.getOrElse(null),
      exportData.h_buckets.getOrElse(null),
      exportData.tscount.getOrElse(null),
      exportData.workspace,
      exportData.namespace,
      exportData.year,
      exportData.month,
      exportData.day,
      exportData.hour,
      exportData.metric
    )
    Row.fromSeq(dataSeq)
  }

  def exportDataToObjectStore(rdd: RDD[Row], spark: SparkSession, settings: DownsamplerSettings): Unit = {
    DownsamplerContext.dsLogger.info(s"exporting chunks to " +
      s"${settings.downsampleUniCatalog}.${settings.downsampleDatabase}.${settings.downsampleTable}")
    val tableLocation = s"${settings.downsampleUniCatalogWarehouse}/" +
      s"${settings.downsampleDatabase}/${settings.downsampleTable}"
    val createStatement = createTableStmt(settings.downsampleDatabase, settings.downsampleTable, tableLocation)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${settings.downsampleDatabase}")
    spark.sql(s"DROP TABLE IF EXISTS ${settings.downsampleDatabase}.${settings.downsampleTable}")
    /*val createStatement = s"CREATE TABLE IF NOT EXISTS " +
      s"${settings.downsampleDatabase}.${settings.downsampleTable} " +
      s" (dsresolution string, partkey binary, chunkid long, chunkinfo binary, workspace string, namespace string," +
      s" metric string, starttime long, endtime long, shard long, numrows long, timestamp binary, schemahash long," +
      s" value binary, avg binary, sum binary, count binary, min binary, max binary, h binary, tscount binary," +
      s" day long, month long, year long, hour long) " +
      s" PARTITIONED BY (workspace, namespace, year, month, day, hour, metric) " +
  s" LOCATION '${settings.downsampleUniCatalogWarehouse}/${settings.downsampleDatabase}/${settings.downsampleTable}'"*/

    spark.sql(createStatement)

    spark.createDataFrame(rdd, this.exportSchema)
      .write
      .mode(SaveMode.Append)
      .format(settings.exportBlobstoreFormat)
      //.partitionBy(partitionByNames: _*)
      .insertInto(s"${settings.downsampleDatabase}.${settings.downsampleTable}")
      //.insertInto(s"${settings.downsampleDatabase}.${settings.downsampleTable}")
  }

  private def createTableStmt(databaseName: String, tableName: String, tablePath: String) =
    s"""
       |CREATE TABLE IF NOT EXISTS $databaseName.$tableName (
       | dsresolution string,
       | partkey binary,
       | chunkid long,
       | chunkinfo binary,
       | starttime long,
       | endtime long,
       | shard long,
       | numrows long,
       | timestamp binary,
       | schemahash long,
       | value binary,
       | avg binary,
       | sum binary,
       | count binary,
       | min binary,
       | max binary,
       | h binary,
       | tscount binary
       |)
       | USING iceberg
       | PARTITIONED BY (workspace string ,namespace string,year long,month long,day long,hour long,metric string)
       | LOCATION '$tablePath'
       |""".stripMargin

  private def byteBufToBytes(byteBuf: ByteBuffer): Array[Byte] = {
    val byteArray = new Array[Byte](byteBuf.position)
    byteBuf.rewind
    byteBuf.get(byteArray)
    byteArray
  }
}
