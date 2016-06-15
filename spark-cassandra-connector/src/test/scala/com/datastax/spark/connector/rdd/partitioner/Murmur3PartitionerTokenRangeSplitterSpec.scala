package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.scalatest.{FlatSpec, Matchers}

import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.Murmur3TokenFactory._
import com.datastax.spark.connector.rdd.partitioner.dht.{LongToken, TokenRange}

class Murmur3PartitionerTokenRangeSplitterSpec
  extends FlatSpec
  with SplitterBehaviors[Long, LongToken]
  with Matchers {

  private val splitter = new Murmur3PartitionerTokenRangeSplitter

  "Murmur3PartitionerSplitter" should "split tokens" in testSplittingTokens(splitter)

  it should "split token sequences" in testSplittingTokenSequences(splitter)

  override def hugeTokens: Seq[TokenRange[Long, LongToken]] = {
    val hugeTokensCount = 10
    val hugeTokensIncrement = totalTokenCount / hugeTokensCount
    (0 until hugeTokensCount).map(i =>
      range(minToken.value + i * hugeTokensIncrement, minToken.value + (i + 1) * hugeTokensIncrement)
    )
  }

  override def range(start: BigInt, end: BigInt): TokenRange[Long, LongToken] =
    new TokenRange[Long, LongToken](
      LongToken(start.toLong),
      LongToken(end.toLong),
      Set(InetAddress.getLocalHost),
      Murmur3TokenFactory)
}