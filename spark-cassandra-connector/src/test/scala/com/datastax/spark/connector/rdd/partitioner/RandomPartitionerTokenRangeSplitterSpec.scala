package com.datastax.spark.connector.rdd.partitioner

import java.net.InetAddress

import org.scalatest.{Matchers, _}

import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.RandomPartitionerTokenFactory
import com.datastax.spark.connector.rdd.partitioner.dht.TokenFactory.RandomPartitionerTokenFactory.{minToken, totalTokenCount}
import com.datastax.spark.connector.rdd.partitioner.dht.{BigIntToken, TokenRange}

class RandomPartitionerTokenRangeSplitterSpec
  extends FlatSpec
    with SplitterBehaviors[BigInt, BigIntToken]
    with Matchers {

  private val splitter = new RandomPartitionerTokenRangeSplitter

  "RandomPartitionerSplitter" should "split tokens" in testSplittingTokens(splitter)

  it should "split token sequences" in testSplittingTokenSequences(splitter)

  override def hugeTokens: Seq[TokenRange[BigInt, BigIntToken]] = {
    val hugeTokensCount = 10
    val hugeTokensIncrement = totalTokenCount / hugeTokensCount
    (0 until hugeTokensCount).map(i =>
      range(minToken.value + i * hugeTokensIncrement, minToken.value + (i + 1) * hugeTokensIncrement)
    )
  }

  override def range(start: BigInt, end: BigInt): TokenRange[BigInt, BigIntToken] =
    new TokenRange[BigInt, BigIntToken](
      BigIntToken(start),
      BigIntToken(end),
      Set(InetAddress.getLocalHost),
      RandomPartitionerTokenFactory)
}
