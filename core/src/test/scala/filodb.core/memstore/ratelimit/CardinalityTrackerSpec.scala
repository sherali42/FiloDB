package filodb.core.memstore.ratelimit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.{DatasetRef, MachineMetricsData}

class CardinalityTrackerSpec extends AnyFunSpec with Matchers {

  val ref = MachineMetricsData.dataset2.ref

  private def newCardStore = {
    new RocksDbCardinalityStore(DatasetRef("test"), 0)
  }

  it("should enforce quota when set explicitly for all levels") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(4, 4, 4, 4), newCardStore)
    t.setQuota(Seq("a", "aa", "aaa"), 1) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(0, 0, 0, 1))
    t.setQuota(Seq("a", "aa"), 2) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 2))
    t.setQuota(Seq("a"), 1) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 1))

    t.modifyCount(Seq("a", "aa", "aaa"), 1, 1) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 1, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 1, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 1, 1, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(1, 1, 1, 1)))

    t.modifyCount(Seq("a", "aa", "aab"), 1, 1) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 2, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 2, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 2, 2, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 1, 1, 4)))

    // aab stopped ingesting
    t.modifyCount(Seq("a", "aa", "aab"), 0, -1) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 1, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 1, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 1, 2, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 0, 1, 4)))

    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aac"), 1, 0)
    }
    ex.prefix shouldEqual (Seq("a", "aa"))

    // increment should not have been applied for any prefix
    t.getCardinality(Seq("a")) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(2, 1, 1, 1))
    t.getCardinality(Seq("a", "aa")) shouldEqual CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 1, 2, 2))
    t.getCardinality(Seq("a", "aa", "aac")) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aac"), CardinalityValue(0, 0, 0, 4))

    // aab was purged
    t.decrementCount(Seq("a", "aa", "aab")) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 1, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 1, 1, 1)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 1, 2, 2)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(0, 0, 0, 4)))

    t.close()
  }

  it("should invoke quota exceeded protocol when breach occurs") {

    class MyQEP extends QuotaExceededProtocol {
      var breachedPrefix: Seq[String] = Nil
      var breachedQuota = -1
      def quotaExceeded(ref: DatasetRef, shard: Int, shardKeyPrefix: Seq[String], quota: Int): Unit = {
        breachedPrefix = shardKeyPrefix
        breachedQuota = quota
      }
    }

    val qp = new MyQEP
    val t = new CardinalityTracker(ref, 0, 3, Seq(1, 1, 1, 1), newCardStore, qp)
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
    qp.breachedQuota shouldEqual 1
    qp.breachedPrefix shouldEqual Seq("a", "aa", "aaa")
    t.close()
  }

  it("should enforce quota when not set for any level") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(4, 4, 4, 4), newCardStore)
    t.modifyCount(Seq("a", "ab", "aba"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 4)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 4)),
        CardinalityRecord(0, Seq("a", "ab"), CardinalityValue(1, 0, 1, 4)),
        CardinalityRecord(0, Seq("a", "ab", "aba"), CardinalityValue(1, 0, 1, 4)))
    t.close()
  }

  it("should be able to enforce for top 2 levels always, and enforce for 3rd level only in some cases") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(20, 20, 20, 20), newCardStore)
    t.setQuota(Seq("a"), 10) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 10))
    t.setQuota(Seq("a", "aa"), 10) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 10))
    // enforce for 3rd level only for aaa
    t.setQuota(Seq("a", "aa", "aaa"), 2) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(0, 0, 0, 2))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(1, 0, 1, 2)))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(2, 0, 2, 2)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(3, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(3, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 0, 1, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(4, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(2, 0, 2, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(5, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(5, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(5, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(3, 0, 3, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(6, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(6, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(6, 0, 2, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 20)))

    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
     ex.prefix shouldEqual Seq("a", "aa", "aaa")
    t.close()

  }

  it("should be able to increase and decrease quota after it has been set before") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(20, 20, 20, 20), newCardStore)
    t.setQuota(Seq("a"), 10) shouldEqual CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 10))
    t.setQuota(Seq("a", "aa"), 10) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 10))
    // enforce for 3rd level only for aaa
    t.setQuota(Seq("a", "aa", "aaa"), 2) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(0, 0, 0, 2))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(1, 0, 1, 2)))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(2, 0, 2, 2)))

    val ex = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
    ex.prefix shouldEqual (Seq("a", "aa", "aaa"))

    // increase quota
    t.setQuota(Seq("a", "aa", "aaa"), 5) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(2, 0, 2, 5))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(3, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(3, 0, 3, 5)))

    // decrease quota
    t.setQuota(Seq("a", "aa", "aaa"), 4) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(3, 0, 3, 4))
    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(4, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aaa"), CardinalityValue(4, 0, 4, 4)))
    val ex2 = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    }
    ex2.prefix shouldEqual Seq("a", "aa", "aaa")
    t.close()
  }

  it("should be able to decrease quota if count is higher than new quota") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(20, 20, 20, 20), newCardStore)
    t.setQuota(Seq("a"), 10) shouldEqual
      CardinalityRecord(0, Seq("a"), CardinalityValue(0, 0, 0, 10))
    t.setQuota(Seq("a", "aa"), 10) shouldEqual
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(0, 0, 0, 10))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(1, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(1, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(1, 0, 1, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(2, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(2, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(2, 0, 2, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(3, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(3, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(3, 0, 3, 20)))
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0) shouldEqual
      Seq(CardinalityRecord(0, Nil, CardinalityValue(4, 0, 1, 20)),
        CardinalityRecord(0, Seq("a"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 1, 10)),
        CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 20)))

    t.getCardinality(Seq("a", "aa", "aab")) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 20))

    t.setQuota(Seq("a", "aa", "aab"), 3)
    t.getCardinality(Seq("a", "aa", "aab")) shouldEqual
      CardinalityRecord(0, Seq("a", "aa", "aab"), CardinalityValue(4, 0, 4, 3))
    val ex2 = intercept[QuotaReachedException] {
      t.modifyCount(Seq("a", "aa", "aab"), 1, 0)
    }
    ex2.prefix shouldEqual Seq("a", "aa", "aab")
    t.close()
  }

  it ("should be able to increment and decrement counters fast and correctly") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(50000, 50000, 50000, 50000), newCardStore)

    (1 to 30000).foreach(_ => t.modifyCount(Seq("a", "b", "c"), 1, 1))
    t.getCardinality(Seq("a", "b", "c")) shouldEqual
      CardinalityRecord(0, Seq("a", "b", "c"), CardinalityValue(30000, 30000, 30000, 50000))
    (1 to 30000).foreach(_ => t.modifyCount(Seq("a", "b", "c"), 0, -1))
    t.getCardinality(Seq("a", "b", "c")) shouldEqual
      CardinalityRecord(0, Seq("a", "b", "c"), CardinalityValue(30000, 0, 30000, 50000))
    (1 to 30000).foreach(_ => t.decrementCount(Seq("a", "b", "c")))
    t.getCardinality(Seq("a", "b", "c")) shouldEqual
      CardinalityRecord(0, Seq("a", "b", "c"), CardinalityValue(0, 0, 0, 50000))
  }

  it ("should be able to do scan") {
    val t = new CardinalityTracker(ref, 0, 3, Seq(100, 100, 100, 100), newCardStore)
    (1 to 10).foreach(_ => t.modifyCount(Seq("a", "ac", "aca"), 1, 0))
    (1 to 20).foreach(_ => t.modifyCount(Seq("a", "ac", "acb"), 1, 0))
    (1 to 11).foreach(_ => t.modifyCount(Seq("a", "ac", "acc"), 1, 0))
    (1 to 6).foreach(_ => t.modifyCount(Seq("a", "ac", "acd"), 1, 0))
    (1 to 1).foreach(_ => t.modifyCount(Seq("a", "ac", "ace"), 1, 0))
    (1 to 9).foreach(_ => t.modifyCount(Seq("a", "ac", "acf"), 1, 0))
    (1 to 15).foreach(_ => t.modifyCount(Seq("a", "ac", "acg"), 1, 0))

    (1 to 15).foreach(_ => t.modifyCount(Seq("b", "bc", "bcg"), 1, 0))
    (1 to 9).foreach(_ => t.modifyCount(Seq("b", "bc", "bch"), 1, 0))
    (1 to 9).foreach(_ => t.modifyCount(Seq("b", "bd", "bdh"), 1, 0))

    (1 to 3).foreach(_ => t.modifyCount(Seq("c", "cc", "ccg"), 1, 0))
    (1 to 2).foreach(_ => t.modifyCount(Seq("c", "cc", "cch"), 1, 0))

    t.modifyCount(Seq("a", "aa", "aaa"), 1, 0)
    t.modifyCount(Seq("a", "aa", "aab"), 1, 0)
    t.modifyCount(Seq("a", "aa", "aac"), 1, 0)
    t.modifyCount(Seq("a", "aa", "aad"), 1, 0)
    t.modifyCount(Seq("b", "ba", "baa"), 1, 0)
    t.modifyCount(Seq("b", "bb", "bba"), 1, 0)
    t.modifyCount(Seq("a", "ab", "aba"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abb"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abc"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abd"), 1, 0)
    t.modifyCount(Seq("a", "ab", "abe"), 1, 0)

    t.scan(Seq("a", "ac"), 3) should contain theSameElementsAs Seq(
      CardinalityRecord(0, Seq("a", "ac", "aca"), CardinalityValue(10, 0, 10, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acb"), CardinalityValue(20, 0, 20, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acc"), CardinalityValue(11, 0, 11, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acd"), CardinalityValue(6, 0, 6, 100)),
      CardinalityRecord(0, Seq("a", "ac", "ace"), CardinalityValue(1, 0, 1, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acf"), CardinalityValue(9, 0, 9, 100)),
      CardinalityRecord(0, Seq("a", "ac", "acg"), CardinalityValue(15, 0, 15, 100)),
    )

    t.scan(Seq("a"), 2) should contain theSameElementsAs Seq(
      CardinalityRecord(0, Seq("a", "aa"), CardinalityValue(4, 0, 4, 100)),
      CardinalityRecord(0, Seq("a", "ab"), CardinalityValue(5, 0, 5, 100)),
      CardinalityRecord(0, Seq("a", "ac"), CardinalityValue(72, 0, 7, 100))
    )

    t.scan(Nil, 1) should contain theSameElementsAs Seq(
      CardinalityRecord(0, Seq("c"), CardinalityValue(5, 0, 1, 100)),
      CardinalityRecord(0, Seq("a"), CardinalityValue(81, 0, 3, 100)),
      CardinalityRecord(0, Seq("b"), CardinalityValue(35, 0, 4, 100))
    )

    t.close()
  }
}