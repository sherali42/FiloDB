package filodb.indexpoc

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}
import monix.reactive.Observable
import org.apache.lucene.util.BytesRef

import filodb.cassandra.CassandraTSStoreFactory
import filodb.coordinator.{FilodbSettings, StoreFactory}
import filodb.core.{GlobalConfig, GlobalScheduler}
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.{FiloSchedulers, IndexData, PartKeyLuceneIndex}
import filodb.core.memstore.FiloSchedulers.IngestSchedName
import filodb.core.memstore.TimeSeriesShard.indexTimeBucketSchema
import filodb.core.metadata.Dataset
import filodb.core.store.MetaStore
import filodb.prometheus.FormatConversion


class IndexLoader extends StrictLogging {
  implicit lazy val ec = GlobalScheduler.globalImplicitScheduler

  val settings = new FilodbSettings(GlobalConfig.systemConfig)

  val dataset: Dataset = FormatConversion.dataset

  val partKeyIndex = new PartKeyLuceneIndex(dataset, 0, 3.days)

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

  private final val numTimeBucketsToRetain = 4 // FIXME

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
        val partId = createPartitionID()

        // add newly assigned partId to lucene index
        partIdMap(partKeyBytesRef) = partId
        partKeyIndex.addPartKey(partKeyBaseOnHeap, partId, startTime, endTime,
          PartKeyLuceneIndex.unsafeOffsetToBytesRefOffset(partKeyOffset))(partKeyNumBytes)
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

  /* We need this map to track partKey->partId because lucene index cannot be looked up
 using partKey efficiently, and more importantly, it is eventually consistent.
  The map and contents will be garbage collected after we are done with recovery */
  val partIdMap = debox.Map.empty[BytesRef, Int]

  def recoverIndex(shardNum: Int): Future[Unit] = {
    val highestIndexTimeBucket = Await.result(metaStore.readHighestIndexTimeBucket(dataset.ref, shardNum), 1.minute)
    val currentIndexTimeBucket = highestIndexTimeBucket.map(_ + 1).getOrElse(0)

    val p = Promise[Unit]()
    Future {
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
        p.success(())
      }(ingestSched)
    }(ingestSched)
    p.future
  }

  def refreshPartKeyIndexBlocking(): Unit = partKeyIndex.refreshReadersBlocking()

  def completeIndexRecovery(): Unit = {
    logger.info(s"start Bootstrapping index for dataset=${dataset.ref}")
    try {
      refreshPartKeyIndexBlocking()
    } catch {
      case e: Exception => logger.error("error in completeIndexRecovery", e)
    }

    logger.info(s"Bootstrapped index for dataset=${dataset.ref}")
  }


}
