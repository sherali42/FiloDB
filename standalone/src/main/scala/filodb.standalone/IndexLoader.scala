package filodb.standalone

import java.util.concurrent.locks.StampedLock

import scala.concurrent.{Future, Promise}

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.reactive.Observable
import org.apache.lucene.util.BytesRef
import org.jctools.maps.NonBlockingHashMapLong

import filodb.cassandra.CassandraTSStoreFactory
import filodb.coordinator.{FilodbSettings, StoreFactory}
import filodb.core.{GlobalConfig, GlobalScheduler}
import filodb.core.binaryrecord2.{RecordContainer, RecordSchema}
import filodb.core.memstore.{FiloSchedulers, IndexData, PartitionSet, PartKeyLuceneIndex}
import filodb.core.memstore.FiloSchedulers.IngestSchedName
import filodb.core.memstore.TimeSeriesShard.indexTimeBucketSchema
import filodb.core.metadata.Dataset
import filodb.core.store.{MetaStore, StoreConfig}

object IndexLoader {
  def partKeyGroup(schema: RecordSchema, partKeyBase: Any, partKeyOffset: Long, numGroups: Int): Int = {
    Math.abs(schema.partitionHash(partKeyBase, partKeyOffset) % numGroups)
  }
  private final case class PartKey(base: Any, offset: Long)
  private final val CREATE_NEW_PARTID = -1
  val InitialNumPartitions = 128 * 1024

  /**
    * Keeps track of the list of partIds of partKeys to store in each index time bucket.
    * This is used to persist the time buckets, and track the partIds to roll over to latest
    * time bucket
    */
  private final val timeBucketBitmaps = new NonBlockingHashMapLong[EWAHCompressedBitmap]()
}

class IndexLoader(dataset: Dataset, storeConfig: StoreConfig) extends StrictLogging {
  import IndexLoader._
  implicit lazy val ec = GlobalScheduler.globalImplicitScheduler

  val partKeyIndex = new PartKeyLuceneIndex(dataset, 0, storeConfig)

  val settings = new FilodbSettings(GlobalConfig.systemConfig)

  lazy val ioPool = Scheduler.io(name = FiloSchedulers.IOSchedName,
    reporter = UncaughtExceptionReporter(
      logger.error("Uncaught Exception in FilodbCluster.ioPool", _)))

  /**
    * next partition ID number
    */
  private var nextPartitionID = 0

  /** Initializes columnStore and metaStore using the factory setting from config. */
  private lazy val factory = StoreFactory(settings, ioPool).asInstanceOf[CassandraTSStoreFactory]

  lazy val metaStore: MetaStore = factory.metaStore

  lazy val memStore = factory.memStore

  lazy val colStore = factory.colStore

  val ingestSched = Scheduler.singleThread(s"$IngestSchedName-${dataset.ref}-0",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in TimeSeriesShard.ingestSched", _)))

  private final val numTimeBucketsToRetain = Math.ceil(4).toInt

  // Use 1/4 of max # buckets for initial ChunkMap size
  private val initInfoMapSize = Math.max((numTimeBucketsToRetain / 4) + 4, 20)

  @volatile
  private var currentIndexTimeBucket = 0

  /**
    * PartitionSet - access TSPartition using ingest record partition key in O(1) time.
    */
  private final val partSet = PartitionSet.ofSize(InitialNumPartitions)
  // Use a StampedLock because it supports optimistic read locking. This means that no blocking
  // occurs in the common case, when there isn't any contention reading from partSet.
  private final val partSetLock = new StampedLock

  // scalastyle:off method.length
  private def extractTimeBucket(segment: IndexData, partIdMap: debox.Map[BytesRef, Int]): Unit = {
    var numRecordsProcessed = 0
    segment.records.iterate(indexTimeBucketSchema).foreach { row =>
      // read binary record and extract the indexable data fields
      val startTime: Long = row.getLong(0)
      val endTime: Long = row.getLong(1)
      val partKeyBaseOnHeap = row.getBlobBase(2).asInstanceOf[Array[Byte]]
      val partKeyOffset = row.getBlobOffset(2)
      val partKeyNumBytes = row.getBlobNumBytes(2)
      val partKeyBytesRef = new BytesRef(partKeyBaseOnHeap,
        PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset(partKeyOffset),
        partKeyNumBytes)

      // look up partKey in partIdMap if it already exists before assigning new partId.
      // We cant look it up in lucene because we havent flushed index yet
      if (partIdMap.get(partKeyBytesRef).isEmpty) {
        val partId = if (endTime == Long.MaxValue) {
          // this is an actively ingesting partition
          val group = partKeyGroup(dataset.partKeySchema, partKeyBaseOnHeap, partKeyOffset, 60)
          Some(createNewPartition(partKeyBaseOnHeap, partKeyOffset, group, CREATE_NEW_PARTID, 4))
        } else {
          Some(createPartitionID())
        }

        // add newly assigned partId to lucene index
        partId.foreach { partId =>
          partIdMap(partKeyBytesRef) = partId
          partKeyIndex.addPartKey(partKeyBaseOnHeap, partId, startTime, endTime,
            PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset(partKeyOffset))(partKeyNumBytes)
          timeBucketBitmaps.get(segment.timeBucket).set(partId)
        }
      } else {
        // partId has already been assigned for this partKey because we previously processed a later record in time.
        // Time buckets are processed in reverse order, and given last one wins and is used for index,
        // we skip this record and move on.
      }
      numRecordsProcessed += 1
    }
    logger.info(s"Recovered partKeys for dataset=${dataset.ref}" +
      s" timebucket=${segment.timeBucket} segment=${segment.segment} numRecordsInBucket=$numRecordsProcessed" +
      s" numPartsInIndex=${partIdMap.size}")
  }

  private def createPartitionID(): Int = {
    val id = nextPartitionID
      nextPartitionID += 1
      if (nextPartitionID < 0) {
        nextPartitionID = 0
        logger.info(s"dataset=${dataset.ref} nextPartitionID has wrapped around to 0 again")
      }
    id
  }

  def recoverIndex(shardNum: Int): Future[Unit] = {
    val p = Promise[Unit]()
    Future {
      val tracer = Kamon.buildSpan("memstore-recover-index-latency")
        .withTag("dataset", dataset.name)
        .withTag("shard", shardNum).start()

      /* We need this map to track partKey->partId because lucene index cannot be looked up
       using partKey efficiently, and more importantly, it is eventually consistent.
        The map and contents will be garbage collected after we are done with recovery */
      val partIdMap = debox.Map.empty[BytesRef, Int]

      val earliestTimeBucket = Math.max(0, currentIndexTimeBucket - numTimeBucketsToRetain)
      logger.info(s"Recovering timebuckets $earliestTimeBucket to ${currentIndexTimeBucket - 1} " +
        s"for dataset=${dataset.ref} shard=$shardNum ")
      // go through the buckets in reverse order to first one wins and we need not rewrite
      // entries in lucene
      // no need to go into currentIndexTimeBucket since it is not present in cass
      val timeBuckets = for {tb <- currentIndexTimeBucket - 1 to earliestTimeBucket by -1} yield {
        colStore.getPartKeyTimeBucket(dataset, shardNum, tb).map { b =>
          new IndexData(tb, b.segmentId, RecordContainer(b.segment.array()))
        }
      }
      Observable.flatten(timeBuckets: _*)
        .foreach(tb => extractTimeBucket(tb, partIdMap))(ingestSched)
        //.map(_ => completeIndexRecovery())(ingestSched)
        .onComplete { _ =>
          tracer.finish()
          p.success(())
        }(ingestSched)
    }(ingestSched)
    p.future
  }

  def refreshPartKeyIndexBlocking(): Unit = partKeyIndex.refreshReadersBlocking()

  def startFlushingIndex(): Unit = partKeyIndex.startFlushThread()

  def completeIndexRecovery(): Unit = {
    logger.info(s"start Bootstrapping index for dataset=${dataset.ref}")
    try {
      refreshPartKeyIndexBlocking()
      startFlushingIndex() // start flushing index now that we have recovered
    } catch {
      case e: Exception => logger.error("error in completeIndexRecovery", e)
    }

    logger.info(s"Bootstrapped index for dataset=${dataset.ref}")
  }

  protected def createNewPartition(partKeyBase: Array[Byte], partKeyOffset: Long,
                                   group: Int, usePartId: Int,
                                   initMapSize: Int = initInfoMapSize): Int = {
    if (usePartId == CREATE_NEW_PARTID) createPartitionID() else usePartId
  }
}
