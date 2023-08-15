package filodb.objectstore.columnstore

import java.util.concurrent.TimeUnit

import scala.concurrent.Future

import com.datastax.driver.core.Session
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.columnstore.{CassandraColumnStore, CassandraTokenRangeSplit}
import filodb.core.{DatasetRef, NotApplied, Response, Success}
import filodb.core.metadata.Schemas
import filodb.core.store.ColumnStore
import filodb.memory.BinaryRegionLarge
import filodb.memory.format.UnsafeUtils

/**
 * Implementation of a column store using S3 compliant blobstore.
 * This class must be thread-safe as it is intended to be used concurrently.
 *
 * ==Constructor Args==
 *
 * @param config see the Configuration section above for the needed config
 * @param readEc A Scheduler for reads.  This must be separate from writes to prevent deadlocks.
 * @param sched A Scheduler for writes
 */
class ObjectStoreColumnStore(override val config: Config, override val readEc: Scheduler,
                             override val session: Session,
                             override val downsampledData: Boolean = false)
                          (override implicit val sched: Scheduler)
extends CassandraColumnStore(config: Config, readEc: Scheduler, session: Session,
    downsampledData: Boolean) with StrictLogging {
  import collection.JavaConverters._

  import filodb.core.store._

  logger.info(s"Starting ObjectStoreColumnStore with config")

  private val writeParallelism = cassandraConfig.getInt("write-parallelism")
  private val writeTimeIndexTtlSeconds = cassandraConfig.getDuration("write-time-index-ttl", TimeUnit.SECONDS).toInt

  /**
   * Shuts down the ColumnStore, including any threads that might be hanging around
   */
  override def shutdown(): Unit = super.shutdown()

  /**
   * Writes the ChunkSets appearing in a stream/Observable to persistent storage, with backpressure
   *
   * @param ref            the DatasetRef for the chunks to write to
   * @param chunksets      an Observable stream of chunksets to write
   * @param diskTimeToLive the time for chunksets to live on disk (Cassandra)
   * @return Success when the chunksets stream ends and is completely written.
   *         Future.failure(exception) if an exception occurs.
   */
  override def write(ref: DatasetRef, chunksets: Observable[ChunkSet], diskTimeToLive: Long): Future[Response] = {
    chunksets.mapParallelUnordered(writeParallelism) { chunkset =>
      val start = System.currentTimeMillis()
      val partBytes = BinaryRegionLarge.asNewByteArray(chunkset.partition)
      val future =
        for {writeChunksResp <- writeChunks(ref, partBytes, chunkset, diskTimeToLive)
             if writeChunksResp == Success
             writeIndicesResp <- writeIndices(ref, partBytes, chunkset, writeTimeIndexTtlSeconds)
             if writeIndicesResp == Success
             } yield {
          writeChunksetLatency.record(System.currentTimeMillis() - start)
          sinkStats.chunksetWrite()
          writeIndicesResp
        }
      Task.fromFuture(future)
    }
      .countL.runToFuture
      .map { chunksWritten =>
        if (chunksWritten > 0) Success else NotApplied
      }
  }

  private def writeChunks(ref: DatasetRef,
                          partition: Array[Byte],
                          chunkset: ChunkSet,
                          diskTimeToLiveSeconds: Long): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(ref)
    println(Schemas.global.part.binSchema.toStringPairs(partition, UnsafeUtils.arayOffset))
    chunkTable.writeChunks(partition, chunkset.info, chunkset.chunks, sinkStats, diskTimeToLiveSeconds)
      .collect {
        case Success => chunkset.invokeFlushListener(); Success
      }
  }

  override def readRawPartitions(ref: DatasetRef,
                        maxChunkTime: Long,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = {
    val chunkTable = getOrCreateChunkTable(ref)
    partMethod match {
      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), Nil) =>
        chunkTable.scanPartitionsBySplit(tokens)
      case _ =>
        val partitions = partMethod match {
          case MultiPartitionScan(p, _) => p
          case SinglePartitionScan(p, _) => Seq(p)
          case p => throw new UnsupportedOperationException(s"PartitionScan $p to be implemented later")
        }
        val (start, end) = if (chunkMethod == AllChunkScan) (minChunkUserTime, maxChunkUserTime)
        else (chunkMethod.startTime - maxChunkTime, chunkMethod.endTime)
        chunkTable.readRawPartitionRange(partitions, start, end)
    }
  }

  /**
   * Initializes the ChunkSink for a given dataset.  Must be called once before writing.
   */
  override def initialize(dataset: DatasetRef, numShards: Int): Future[Response] = super.initialize(dataset, numShards)

}
