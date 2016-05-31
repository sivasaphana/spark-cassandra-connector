package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.BigIntToken

/** Fast token range splitter assuming that data are spread out evenly in the whole range. */
private[partitioner] class RandomPartitionerTokenRangeSplitter
  extends TokenRangeSplitter[BigInt, BigIntToken] {

  private type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[BigInt, BigIntToken]

  private def wrapWithMax(max: BigInt)(token: BigInt): BigInt = {
    if (token <= max) token else token - max
  }

  override def split(tokenRange: TokenRange, splitCount: Int): Seq[TokenRange] = {
    val rangeTokenCount = tokenRange.distance
    val wrap = wrapWithMax(tokenRange.tokenFactory.maxToken.value)(_)

    val splitPointsCount = if (rangeTokenCount < splitCount) rangeTokenCount.toInt else splitCount
    val splitPoints = (0 until splitPointsCount).map({ i =>
      val nextToken: BigInt = tokenRange.start.value + (rangeTokenCount * i / splitPointsCount)
      new BigIntToken(wrap(nextToken))
    }) :+ tokenRange.end

    for (Seq(left, right) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange(left, right, tokenRange.replicas, tokenRange.tokenFactory)
  }
}