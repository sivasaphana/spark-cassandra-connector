package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenRange}
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

private[partitioner] trait SplitterBehaviors[V, T <: Token[V]] {
  this: TableDrivenPropertyChecks with Matchers =>

  case class SplitResult(splitCount: Int, minSize: BigInt, maxSize: BigInt)

  def range(start: BigInt, end: BigInt): TokenRange[V, T]

  def hugeTokens: Seq[TokenRange[V, T]]

  def singleTokenSplitter(splitter: TokenRangeSplitter[V, T]) {
    forAll(Table(("tokenRange", "desiredSplitCount", "expectedResult"),

      (range(start = 0, end = 1), splittedIn(1), outputs(splits = 1, withSize = 1)),
      (range(start = 0, end = 10), splittedIn(1), outputs(splits = 1, withSize = 10)),
      (range(start = 0, end = 1), splittedIn(10), outputs(splits = 1, withSize = 1)),

      (range(start = 0, end = 9), splittedIn(10), outputs(splits = 9, withSize = 1)),
      (range(start = 0, end = 10), splittedIn(10), outputs(splits = 10, withSize = 1)),
      (range(start = 0, end = 11), splittedIn(10), outputs(splits = 10, withSize = 1, sizeTolerance = 1)),

      (range(start = 10, end = 50), splittedIn(10), outputs(splits = 10, withSize = 4)),
      (range(start = 0, end = 1000), splittedIn(3), outputs(splits = 3, withSize = 333, sizeTolerance = 1)),
      (hugeTokens.head, splittedIn(100), outputs(splits = 100, withSize = hugeTokens.head.distance / 100, sizeTolerance = 4))

    )) { (tokenRange: TokenRange[V, T], desiredSplitCount: Int, expected: SplitResult) =>

      val splits = splitter.split(tokenRange, desiredSplitCount)

      splits.size should be(expected.splitCount)
      splits.head.start should be(tokenRange.start)
      splits.last.end should be(tokenRange.end)
      splits.foreach(_.replicas should be(tokenRange.replicas))
      splits.foreach(s => s.distance should (be >= expected.minSize and be <= expected.maxSize))
      splits.map(_.distance).sum should be(tokenRange.distance)
      splits.map(_.ringFraction).sum should be(tokenRange.ringFraction +- .000000001)
      for (Seq(range1, range2) <- splits.sliding(2)) range1.end should be(range2.start)
    }
  }

  def multipleTokenSplitter(splitter: TokenRangeSplitter[V, T]) {
    forAll(Table(("tokenRanges", "desiredSplitCount", "expectedResult"),

      (Seq(range(start = 0, end = 1)), splittedIn(1), outputs(splits = 1, withSize = 1)),
      (Seq(range(start = 0, end = 1), range(start = 1, end = 2)),
        splittedIn(1), outputs(splits = 2, withSize = 1)),
      (Seq(range(start = 0, end = 10), range(start = 10, end = 20)),
        splittedIn(4), outputs(splits = 4, withSize = 5)),
      (hugeTokens,
        splittedIn(1000),
        outputs(splits = 1000, withSize = hugeTokens.map(_.distance).sum / 1000, sizeTolerance = 1000))

    )) { (tokenRanges: Seq[TokenRange[V, T]], desiredSplitCount: Int, expected: SplitResult) =>

      val splits = splitter.split(tokenRanges, desiredSplitCount)

      splits.size should be(expected.splitCount)
      splits.head.start should be(tokenRanges.head.start)
      splits.last.end should be(tokenRanges.last.end)
      splits.foreach(s => s.distance should (be >= expected.minSize and be <= expected.maxSize))
      splits.map(_.distance).sum should be(tokenRanges.map(_.distance).sum)
      splits.map(_.ringFraction).sum should be(tokenRanges.map(_.ringFraction).sum +- .000000001)
      for (Seq(range1, range2) <- splits.sliding(2)) range1.end should be(range2.start)
    }
  }

  def splittedIn(splitCount: Int): Int = splitCount

  def outputs(splits: Int, withSize: BigInt, sizeTolerance: BigInt = BigInt(0)): SplitResult =
    new SplitResult(splits, withSize - sizeTolerance, withSize + sizeTolerance)
}
