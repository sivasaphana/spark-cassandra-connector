package com.datastax.spark.connector.rdd.partitioner

import com.datastax.spark.connector.rdd.partitioner.dht.LongToken

/** Fast token range splitter assuming that data are spread out evenly in the whole range. */
private[partitioner] class Murmur3PartitionerTokenRangeSplitter
  extends TokenRangeSplitter[Long, LongToken] {

  private type TokenRange = com.datastax.spark.connector.rdd.partitioner.dht.TokenRange[Long, LongToken]

  override def split(tokenRange: TokenRange, splitCount: Int): Seq[TokenRange] = {
    val rangeTokenCount = tokenRange.distance
    val splitPointsCount = if (rangeTokenCount < splitCount) rangeTokenCount.toInt else splitCount
    val splitPoints = (0 until splitPointsCount).map({ i =>
      new LongToken(tokenRange.start.value + (rangeTokenCount * i / splitPointsCount).toLong)
    }) :+ tokenRange.end

    for (Seq(left, right) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange(left, right, tokenRange.replicas, tokenRange.tokenFactory)
  }
}
