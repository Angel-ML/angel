package com.tencent.angel.spark.ml.tree.util

import org.apache.spark.Partitioner

class EvenPartitioner(numKey: Int, numPartition: Int) extends Partitioner {
  private val numFeatPerPart = if (numKey % numPartition > numKey / 2) {
    Math.ceil(1.0 * numKey / numPartition).toInt
  } else {
    Math.floor(1.0 * numKey / numPartition).toInt
  }

  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = Math.min(key.asInstanceOf[Int] / numFeatPerPart, numPartition - 1)

  def partitionEdges(): Array[Int] = {
    val res = new Array[Int](numPartition + 1)
    for (i <- 0 until numPartition)
      res(i) = i * numFeatPerPart
    res(numPartition) = numKey
    res
  }
}

class IdenticalPartitioner(numPartition: Int) extends Partitioner {
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    val partId = key.asInstanceOf[Int]
    require(partId < numPartition, s"Partition id $partId exceeds maximum partition num $numPartition")
    partId
  }
}