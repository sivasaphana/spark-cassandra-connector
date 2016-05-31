package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory, TokenRange}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool


/** Splits a token ranges into smaller sub-ranges,
  * each with the desired approximate number of rows. */
private[partitioner] trait TokenRangeSplitter[V, T <: Token[V]] {

  def split(tokenRanges: Iterable[TokenRange[V, T]], splitCount: Int): Iterable[TokenRange[V, T]] = {
    val wholeRing = tokenRanges.map(_.ringFraction).sum
    val ringFractionPerSplit = wholeRing / splitCount.toDouble
    val parTokenRanges = tokenRanges.par

    parTokenRanges.tasksupport = new ForkJoinTaskSupport(TokenRangeSplitter.pool)
    parTokenRanges.flatMap(tokenRange => {
      val n = math.max(1, (tokenRange.ringFraction / ringFractionPerSplit).toInt)
      split(tokenRange, n)
    }).toList
  }

  /** Splits the token range uniformly into sub-ranges. */
   def split(tokenRange: TokenRange[V, T], totalSplitCount: Int): Seq[TokenRange[V, T]]
}

object TokenRangeSplitter {
  private val MaxParallelism = 16

  private val pool = new ForkJoinPool(MaxParallelism)
}
