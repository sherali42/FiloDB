package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler

import filodb.coordinator.{ActorPlanDispatcher, ShardMapper}
import filodb.core.{DatasetRef, MetricsTestData}
import filodb.core.metadata.Schemas
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SinglePartitionPlannerSpec extends AnyFunSpec with Matchers {
  private implicit val system = ActorSystem()
  private val node = TestProbe().ref

  private val localMapper = new ShardMapper(32)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  private val remoteMapper = new ShardMapper(16)
  for {i <- 0 until 32} localMapper.registerNode(Seq(i), node)

  private val dataset = MetricsTestData.timeseriesDataset
  private val dsRef = dataset.ref
  private val schemas = Schemas(dataset.schema)

  private val routingConfigString = "routing {\n  remote {\n   " +
    " http {\n      endpoint = localhost\n timeout = 10000\n    }\n  }\n}"

  private val routingConfig = ConfigFactory.parseString(routingConfigString)
  private val config = ConfigFactory.load("application_test.conf").getConfig("filodb.query").
    withFallback(routingConfig)
  private val queryConfig = QueryConfig(config)

  private val promQlQueryParams = PromQlQueryParams("sum(heap_usage)", 100, 1, 1000)

  val localPlanner = new SingleClusterPlanner(dataset, schemas, localMapper, earliestRetainedTimestampFn = 0, queryConfig,
    "raw")
  val remotePlanner = new SingleClusterPlanner(dataset, schemas, remoteMapper, earliestRetainedTimestampFn = 0,
    queryConfig, "raw")

  val failureProvider = new FailureProvider {
    override def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange] = {
      Seq(FailureTimeRange("local", datasetRef,
        TimeRange(300000, 400000), false))
    }
  }

  val highAvailabilityPlanner = new HighAvailabilityPlanner(dsRef, localPlanner, failureProvider, queryConfig)

  class MockExecPlan(val name: String, val lp: LogicalPlan) extends ExecPlan {
    override def queryContext: QueryContext = QueryContext()
    override def children: Seq[ExecPlan] = Nil
    override def submitTime: Long = 1000
    override def dataset: DatasetRef = ???
    override def dispatcher: PlanDispatcher = InProcessPlanDispatcher(queryConfig)
    override def doExecute(source: ChunkSource,
                           querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = ???
    override protected def args: String = "mock-args"
  }

   val rrPlanner1 = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("rules1", logicalPlan)
    }
  }

  val rrPlanner2 = new QueryPlanner {
    override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      new MockExecPlan("rules2", logicalPlan)
    }
  }

  val planners = Map("local" -> highAvailabilityPlanner, "rules1" -> rrPlanner1, "rules2" -> rrPlanner2)
  val plannerSelector = (metricName: String) => { if (metricName.equals("rr1")) "rules1"
  else if (metricName.equals("rr2")) "rules2" else "local" }

  val engine = new SinglePartitionPlanner(planners, "local", plannerSelector, dataset, queryConfig)

  it("should generate Exec plan for simple query") {
    val lp = Parser.queryToLogicalPlan("test{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual (true)
    execPlan.children.length shouldEqual 2
    execPlan.children.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
    execPlan.children.head.rangeVectorTransformers.head.isInstanceOf[PeriodicSamplesMapper] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan") {
    val lp = Parser.queryToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true // Since all metrics belong to same cluster
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.children.foreach { l1 =>
        l1.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
        l1.rangeVectorTransformers.size shouldEqual 1
        l1.rangeVectorTransformers(0).isInstanceOf[PeriodicSamplesMapper] shouldEqual true
      }
    }


  it("should generate exec plan for nested Binary Join query") {
    val lp = Parser.queryToLogicalPlan("test1{job = \"app\"} + test2{job = \"app\"} + test3{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))

    execPlan.dispatcher.isInstanceOf[ActorPlanDispatcher] shouldEqual true // Since all metrics belong to same cluster
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[BinaryJoinExec] shouldEqual true
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.isInstanceOf[MultiSchemaPartitionsExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan with remote and local cluster metrics") {
    val lp = Parser.queryToLogicalPlan("test{job = \"app\"} + rr1{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    // rr1 and test belong to different clusters
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.isInstanceOf[LocalPartitionDistConcatExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec plan with remote cluster metrics") {
    val lp = Parser.queryToLogicalPlan("rr1{job = \"app\"} + rr2{job = \"app\"}", 1000, 1000)

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    // rr1 and rr2 belong to different clusters
    execPlan.dispatcher.isInstanceOf[InProcessPlanDispatcher] shouldEqual true
    execPlan.isInstanceOf[BinaryJoinExec] shouldEqual (true)
    execPlan.asInstanceOf[BinaryJoinExec].lhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[BinaryJoinExec].rhs.head.asInstanceOf[MockExecPlan].name shouldEqual ("rules2")
  }

  it("should generate Exec plan for Metadata query") {
    val lp = Parser.metadataQueryToLogicalPlan("http_requests_total{job=\"prometheus\", method=\"GET\"}",
      TimeStepParams(1000, 10, 2000))

    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[PartKeysDistConcatExec] shouldEqual (true)
    execPlan.asInstanceOf[PartKeysDistConcatExec].children.length shouldEqual(3)

    // For Raw
    execPlan.asInstanceOf[PartKeysDistConcatExec].children(0).isInstanceOf[PartKeysDistConcatExec] shouldEqual true
    execPlan.asInstanceOf[PartKeysDistConcatExec].children(0).asInstanceOf[PartKeysDistConcatExec].children.
      forall(_.isInstanceOf[PartKeysExec]) shouldEqual true

    execPlan.asInstanceOf[PartKeysDistConcatExec].children(1).asInstanceOf[MockExecPlan].name shouldEqual ("rules1")
    execPlan.asInstanceOf[PartKeysDistConcatExec].children(2).asInstanceOf[MockExecPlan].name shouldEqual ("rules2")
  }

  it("should generate correct ExecPlan for TsCardinalities") {

    // Note: this test is expected to break when TsCardinalities.isRoutable = true
    // Note: unrelated to the above, this test is setup to confirm that a hacky fix to
    //   SPP::materializeTsCardinalities is working. See there for additional details.

    val localPlanner = new SingleClusterPlanner(
      dataset, schemas, localMapper, earliestRetainedTimestampFn = 0, queryConfig, "raw-temp")
    val planners = Map("raw-temp" -> localPlanner, "rules1" -> rrPlanner1, "rules2" -> rrPlanner2)
    val engine = new SinglePartitionPlanner(planners, "raw-temp", plannerSelector, dataset, queryConfig)
    val lp = TsCardinalities(Seq("a", "b"), 2)

    // Plan should just contain a single root TsCardReduceExec and its TsCardExec children.
    // Currently, queries are routed only to the planner who's name equals the SPP's "defaultPlanner" member.
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams.copy(promQl = "")))
    execPlan.isInstanceOf[TsCardReduceExec] shouldEqual (true)
    execPlan.asInstanceOf[TsCardReduceExec].children.length shouldEqual(32)
    execPlan.children.forall(_.isInstanceOf[TsCardExec]) shouldEqual true
  }

  it("should generate Exec plan for Scalar query which does not have any metric") {
    val lp = Parser.queryToLogicalPlan("time()", 1000, 1000)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[TimeScalarGeneratorExec] shouldEqual true
  }

  it("should generate BinaryJoin Exec with remote exec's having lhs and rhs query") {
    val lp = Parser.queryRangeToLogicalPlan("""test1{job = "app"} + test2{job = "app"}""", TimeStepParams(300, 20, 500))
    val promQlQueryParams = PromQlQueryParams("test1{job = \"app\"} + test2{job = \"app\"}", 300, 20, 500)
    val execPlan = engine.materialize(lp, QueryContext(origQueryParams = promQlQueryParams))
    execPlan.isInstanceOf[PromQlRemoteExec] shouldEqual (true)
    execPlan.asInstanceOf[PromQlRemoteExec].queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      promQl shouldEqual("""test1{job = "app"} + test2{job = "app"}""")
  }
}

