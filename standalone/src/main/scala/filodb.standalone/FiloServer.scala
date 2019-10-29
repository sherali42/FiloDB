package filodb.standalone

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.ActorRef
import akka.cluster.Cluster
import akka.dispatch.Futures
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

import filodb.akkabootstrapper.AkkaBootstrapper
import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.core.{GlobalConfig, GlobalScheduler}
import filodb.core.metadata.Dataset
import filodb.core.store.StoreConfig
import filodb.http.FiloHttpServer
import net.ceedubs.ficus.Ficus._

/**
 * FiloServer starts a "standalone" FiloDB server which can ingest and support queries through the Akka
 * API.  It is meant to be used in a cluster.
 *
 * - The servers connect to each other setting up an Akka Cluster.  Seed nodes must be configured.
 * - Ingestion must be started using the CLI and the source configured.  When it starts it does nothing
 *   at the beginning.
 *
 * ## Configuration ##
 * {{{
 *   seed-nodes = ["akka.tcp://filo-standalone@hostname_or_ip:2552"]
 *   dataset-definitions {
 *     sample-timeseries {
 *       partition-columns = ["metricName:string", "tags:map"]
 *       data-columns = ["timestamp:long", "value:double"]
 *       row-keys = ["timestamp"]
 *     }
 *   }
 * }}}
 *
 * @param watcher optionally register a watcher `ActorRef` with the `NodeClusterActor` cluster
 *                singleton. Primarily for Multi-JVM tests, but this strategy is used in the
 *                coordinator module in all test types.
 */
class FiloServer(watcher: Option[ActorRef]) extends FilodbClusterNode {

  def this() = this(None)
  def this(watcher: ActorRef) = this(Some(watcher))

  override val role = ClusterRole.Server

  lazy val config = cluster.settings.config

  var filoHttpServer: FiloHttpServer = _

  // Now, initialize any datasets using in memory MetaStore.
  // This is a hack until we are able to use CassandraMetaStore for standalone.  It is also a
  // convenience for users to get up and running quickly without setting up cassandra.
  val client = new LocalClient(coordinatorActor)

  def bootstrap(akkaCluster: Cluster): AkkaBootstrapper = {
    val bootstrapper = AkkaBootstrapper(akkaCluster)
    bootstrapper.bootstrap()
    bootstrapper
  }

  def start(): Unit = {
    try {
      coordinatorActor
      scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
      val bootstrapper = bootstrap(cluster.cluster)
      val singleton = cluster.clusterSingleton(role, watcher)
      filoHttpServer = new FiloHttpServer(cluster.system)
      filoHttpServer.start(coordinatorActor, singleton, bootstrapper.getAkkaHttpRoute())
      // Launch the profiler after startup, if configured.
      SimpleProfiler.launch(systemConfig.getConfig("filodb.profiler"))
    } catch {
      // if there is an error in the initialization, we need to fail fast so that the process can be rescheduled
      case NonFatal(e) =>
        logger.error("Could not initialize server", e)
        shutdown()
    }
  }

  override def shutdown(): Unit = {
    filoHttpServer.shutdown(5.seconds) // TODO configure
    super.shutdown()
  }

  def shutdownAndExit(code: Int): Unit = {
    shutdown()
    sys.exit(code)
  }
}

object FiloServer extends StrictLogging {
  lazy val config = GlobalConfig.systemConfig
  def main(args: Array[String]): Unit = {
    //implicit val global = ExecutionContext.global
    import GlobalScheduler.globalImplicitScheduler
    logger.info("starting the server...")
    val filodbConfig = config.getConfig("filodb")
    logger.info(s"filodb-config $filodbConfig")
    val datasetConfigFiles = filodbConfig.as[Seq[String]]("dataset-configs")
    val datasetConfig = ConfigFactory.parseFile(new java.io.File(datasetConfigFiles(0)))
    val dataset = Dataset.fromConfig(datasetConfig)
    val storeConfig = StoreConfig(datasetConfig.getConfig("sourceconfig.store"))
    val indexLoader = new IndexLoader(dataset, storeConfig)
    val futures = (for {
      shard <- 0 until 256
    } yield indexLoader.recoverIndex(shard)
      .map(_ => logger.info(s"completed index recovery for shard=${shard}"))).asJava
    Futures.sequence(futures, globalImplicitScheduler).onComplete(_ => logger.info("completed loading index"))
  }
}
