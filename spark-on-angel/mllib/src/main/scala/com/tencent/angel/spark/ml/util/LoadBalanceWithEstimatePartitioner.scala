package com.tencent.angel.spark.ml.util

import com.clearspring.analytics.stream.cardinality.{HyperLogLogPlus, ICardinality}
import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner.numBits
import org.apache.spark.rdd.RDD


object LoadBalanceWithEstimatePartitioner {

  class EstimatorCounter(var estimator: HyperLogLogPlus) extends Serializable {
    var counter: Long = 0
  }

  def getBuckets(data: RDD[Long], bits: Int): Array[(Long, EstimatorCounter)] = {
    val buckets = data.map(f => (f >> bits, f))
      .aggregateByKey(new EstimatorCounter(new HyperLogLogPlus(16)))(append, merge).sortBy(_._1).collect()
    buckets
  }

  def append(estimatorCounter: EstimatorCounter, nodeId: Long) = {
    estimatorCounter.estimator.offer(nodeId)
    estimatorCounter.counter = estimatorCounter.counter + 1
    estimatorCounter
  }

  def merge(estimatorCounter1: EstimatorCounter, estimatorCounter2: EstimatorCounter) = {
    estimatorCounter1.estimator = estimatorCounter1.estimator.merge(estimatorCounter2.estimator).asInstanceOf[HyperLogLogPlus]
    estimatorCounter1.counter += estimatorCounter2.counter
    estimatorCounter1
  }

  def partition(buckets: Array[(Long, EstimatorCounter)], ctx: MatrixContext, bits: Int, numPartitions: Int): Unit = {
    println(s"bucket.size=${buckets.size}")
    val sorted = buckets.sortBy(f => f._1)
    val sum = sorted.map(f => f._2.counter).sum
    val per = (sum / numPartitions)

    var start = ctx.getIndexStart
    val end = ctx.getIndexEnd
    val rowNum = ctx.getRowNum

    var current = 0L
    val size = sorted.size
    val limit = ((end.toDouble - start.toDouble) / numPartitions).toLong * 4
    println(limit)

    var estimator: ICardinality = new HyperLogLogPlus(16)
    for (i <- 0 until size) {
      estimator = estimator.merge(sorted(i)._2.estimator)
      // keep each partition similar load and limit the range of each partition
      if (current > per || ((sorted(i)._1 << bits - start > limit) && current > per / 2)) {
        val part = new PartContext(0, rowNum, start, sorted(i)._1 << bits, estimator.cardinality().toInt)
        println(s"part=${part} load=${current} range=${part.getEndCol - part.getStartCol} index number = ${part.getIndexNum}")
        ctx.addPart(part)
        start = sorted(i)._1 << bits
        current = 0L
        estimator = new HyperLogLogPlus(16)
      }
      current += sorted(i)._2.counter
    }

    val part = new PartContext(0, rowNum, start, end, estimator.cardinality().toInt)
    ctx.addPart(part)
    println(s"part=${part} load=${current} range=${end - start} index number=${estimator.cardinality().toInt}")
    println(s"split matrix ${ctx.getName} into ${ctx.getParts.size()} partitions")
  }

  def partition(index: RDD[Long], maxId: Long, psPartitionNum: Int, ctx: MatrixContext, percent: Float = 0.7f): Unit = {
    var p = percent
    var count = 2
    while (count > 0) {
      val bits = (numBits(maxId) * percent).toInt
      println(s"bits used for load balance partition is $bits")
      val buckets = getBuckets(index, bits)
      val sum = buckets.map(f => f._2.counter).sum
      val max = buckets.map(f => f._2.counter).max
      val size = buckets.size
      println(s"max=$max sum=$sum size=$size")
      if (count == 1 || max.toDouble / sum < 0.1) {
        count = 0
        partition(buckets, ctx, bits, psPartitionNum)
      } else {
        count -= 1
        p -= 0.05f
      }
    }
  }
}


