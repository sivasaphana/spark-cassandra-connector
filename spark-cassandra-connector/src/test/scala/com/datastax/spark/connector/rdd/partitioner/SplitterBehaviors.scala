package com.datastax.spark.connector.rdd.partitioner

import org.scalatest.Matchers

import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenRange}

private[partitioner] trait SplitterBehaviors[V, T <: Token[V]] {
  this: Matchers =>

  case class SplitResult(splitCount: Int, minSize: BigInt, maxSize: BigInt)

  def hugeTokens: Seq[TokenRange[V, T]]

  def range(start: BigInt, end: BigInt): TokenRange[V, T]

  def toString(range: TokenRange[V, T]): String = s"(${range.start}, ${range.end})"

  def splittedIn(splitCount: Int): Int = splitCount

  def outputs(splits: Int, withSize: BigInt, sizeTolerance: BigInt = BigInt(0)): SplitResult =
    SplitResult(splits, withSize - sizeTolerance, withSize + sizeTolerance)

  def testSplittingTokens(splitter: => TokenRangeSplitter[V, T]) {

    val splitCases = Seq[(TokenRange[V, T], Int, SplitResult)](
      (range(start = 0, end = 1), splittedIn(1), outputs(splits = 1, withSize = 1)),
      (range(start = 0, end = 10), splittedIn(1), outputs(splits = 1, withSize = 10)),
      (range(start = 0, end = 1), splittedIn(10), outputs(splits = 1, withSize = 1)),

      (range(start = 0, end = 9), splittedIn(10), outputs(splits = 9, withSize = 1)),
      (range(start = 0, end = 10), splittedIn(10), outputs(splits = 10, withSize = 1)),
      (range(start = 0, end = 11), splittedIn(10), outputs(splits = 10, withSize = 1, sizeTolerance = 1)),

      (range(start = 10, end = 50), splittedIn(10), outputs(splits = 10, withSize = 4)),
      (range(start = 0, end = 1000), splittedIn(3), outputs(splits = 3, withSize = 333, sizeTolerance = 1)),
      (hugeTokens.head, splittedIn(100), outputs(splits = 100, withSize = hugeTokens.head.rangeSize / 100, sizeTolerance = 4))
    )

    for ((range, splittedIn, expected) <- splitCases) {
      val splits = splitter.split(range, splittedIn)

      withClue(s"Splitting range ${toString(range)} in $splittedIn splits failed.") {
        splits.size should be(expected.splitCount)
        splits.head.start should be(range.start)
        splits.last.end should be(range.end)
        splits.foreach(_.replicas should be(range.replicas))
        splits.foreach(s => s.rangeSize should (be >= expected.minSize and be <= expected.maxSize))
        splits.map(_.rangeSize).sum should be(range.rangeSize)
        splits.map(_.ringFraction).sum should be(range.ringFraction +- .000000001)
        for (Seq(range1, range2) <- splits.sliding(2)) range1.end should be(range2.start)
      }
    }
  }

  def testSplittingTokenSequences(splitter: TokenRangeSplitter[V, T]) {

    val splitCases = Seq[(Seq[TokenRange[V, T]], Int, SplitResult)](
      (Seq(range(start = 0, end = 1)), splittedIn(1), outputs(splits = 1, withSize = 1)),
      (Seq(range(start = 0, end = 1), range(start = 1, end = 2)),
        splittedIn(1), outputs(splits = 2, withSize = 1)),
      (Seq(range(start = 0, end = 10), range(start = 10, end = 20)),
        splittedIn(4), outputs(splits = 4, withSize = 5)),
      (hugeTokens,
        splittedIn(1000),
        outputs(splits = 1000, withSize = hugeTokens.map(_.rangeSize).sum / 1000, sizeTolerance = 1000))
    )

    for ((ranges, splittedIn, expected) <- splitCases) {
      val splits = splitter.split(ranges, splittedIn)
      withClue(s"Splitting ranges ${ranges.map(toString).mkString(",")} in $splittedIn splits failed.") {
        splits.size should be(expected.splitCount)
        splits.head.start should be(ranges.head.start)
        splits.last.end should be(ranges.last.end)
        splits.foreach(s => s.rangeSize should (be >= expected.minSize and be <= expected.maxSize))
        splits.map(_.rangeSize).sum should be(ranges.map(_.rangeSize).sum)
        splits.map(_.ringFraction).sum should be(ranges.map(_.ringFraction).sum +- .000000001)
        for (Seq(range1, range2) <- splits.sliding(2)) range1.end should be(range2.start)
      }
    }

  }
}
