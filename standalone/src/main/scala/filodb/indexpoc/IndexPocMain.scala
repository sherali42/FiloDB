package filodb.indexpoc

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import akka.dispatch.Futures
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.core.query.{ColumnFilter, Filter}

object IndexPocMain extends App with StrictLogging {

  implicit lazy val globalImplicitScheduler = Scheduler.computation(
    name = "global-implicit",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in GlobalScheduler", _)))
  val indexLoader = new IndexLoader()
  val futures = (for {
    shard <- 0 until 256
  } yield indexLoader.recoverIndex(shard)
    .map(_ => logger.info(s"completed index recovery for shard=${shard}"))).asJava
  Futures.sequence(futures, globalImplicitScheduler).onComplete(_ => logger.info("completed loading index"))


  val endTime = System.currentTimeMillis()
  val startTime = endTime - 7.days.toMillis

  val index = indexLoader.partKeyIndex

  val q1 = Seq(ColumnFilter("__name__", Filter.Equals("mesos_master_slaves_state")),
               ColumnFilter("_ns", Filter.Equals("mesos_master")),
               ColumnFilter("cluster", Filter.Equals("usprz06")),
               ColumnFilter("state", Filter.Equals("connected_active")))

  val q2 = Seq(ColumnFilter("__name__", Filter.Equals("up")),
               ColumnFilter("_ns", Filter.Equals("node")),
               ColumnFilter("instance", Filter.EqualsRegex("m(agent|slave).*.$cluster:.*")),
               ColumnFilter("state", Filter.EqualsRegex("connected_active")))

  val q3 = Seq( ColumnFilter("__name__", Filter.Equals("mesos_master_task_states_exit_total")),
                ColumnFilter("_ns", Filter.Equals("mesos_master")),
                ColumnFilter("cluster", Filter.Equals("usprz06")),
                ColumnFilter("state", Filter.NotEquals("killing")))

  val q4 = Seq( ColumnFilter("__name__", Filter.Equals("num_ingesting_partitions")),
                ColumnFilter("_ns", Filter.Equals("mosaic-prod-sat001-filodb-timeseries")))


  val q5 = Seq( ColumnFilter("__name__", Filter.Equals("nsedge")),
                ColumnFilter("_ns", Filter.Equals("icloud-slo")),
                ColumnFilter("service", Filter.Equals("quotaservice")),
                ColumnFilter("job", Filter.Equals("hubble-slo-kpis")),
                ColumnFilter("metric_name", Filter.Equals("Num upstream 2xx responses")),
                ColumnFilter("summary_type", Filter.Equals("total/sec")))

  val q6 = Seq( ColumnFilter("__name__", Filter.Equals("nsedge")),
                ColumnFilter("_ns", Filter.Equals("icloud-slo")),
                ColumnFilter("service", Filter.Equals("ckdatabase")),
                ColumnFilter("job", Filter.Equals("hubble-slo-kpis")),
                ColumnFilter("metric_name", Filter.Equals("Num upstream 2xx responses")),
                ColumnFilter("summary_type", Filter.Equals("total/sec")))

  var min = Long.MaxValue
  var max = Long.MinValue
  var sum = 0L
  var count = 0L

  0.to(1000).foreach { _ =>
    Seq(q1, q2, q3, q4, q5, q6).foreach { q =>
      val start = System.nanoTime()
      val res = index.partIdsFromFilters2(q, startTime, endTime)
      val dur = System.nanoTime() - start
      count += 1
      min = Math.min(min, dur)
      max = Math.max(max, dur)
      sum += dur
      if (res.cardinality() == 0) logger.info(s"Empty result for $q")
    }
  }

  logger.info(s"Min = $min ns")
  logger.info(s"Max = $max ns")
  logger.info(s"Sum = $sum ns")
  logger.info(s"Cnt = $count")

}

