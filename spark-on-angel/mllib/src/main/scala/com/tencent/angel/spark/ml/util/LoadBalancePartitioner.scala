package com.tencent.angel.spark.ml.util

import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import com.tencent.angel.ml.math2.vector.{LongDummyVector, LongKeyVector, Vector}
import org.apache.spark.rdd.RDD

abstract class AutoPartitioner(bits: Int, numPartitions: Int) extends Serializable {

  def getBuckets(data: RDD[Vector]): Array[(Long, Long)]

  def partition(data: RDD[Vector], ctx: MatrixContext): Unit = {
    val buckets = getBuckets(data)
    val sorted = buckets.sortBy(f => f._1)
    val sum = sorted.map(f => f._2).sum
    val per = (sum / numPartitions)

    var start = ctx.getIndexStart
    val end = ctx.getIndexEnd
    val rowNum = ctx.getRowNum

    var current = 0L
    val size = sorted.size
    val limit = ((end.toDouble - start.toDouble) / numPartitions).toLong * 4
    println(limit)
    for (i <- 0 until size) {
      if (current > per || ((sorted(i)._1 << bits - start > limit) && current > per / 2)) {
        val part = new PartContext(0, rowNum, start, sorted(i)._1 << bits, 0)
        println(s"part=${part} load=${current} range=${part.getEndCol - part.getStartCol}")
        ctx.addPart(part)
        start = sorted(i)._1 << bits
        current = 0L
      }
      current += sorted(i)._2
    }

    val part = new PartContext(0, rowNum, start, end, 0)
    ctx.addPart(part)
    println(s"part=${part} cost=${current} range=${end - start}")
    println(s"split matrix ${ctx.getName} into ${ctx.getParts.size()} partitions")
  }
}

class LoadBalancePartitioner(bits: Int, numPartitions: Int)
  extends AutoPartitioner(bits, numPartitions) {

  override def getBuckets(data: RDD[Vector]): Array[(Long, Long)] = {
    val indices = data.flatMap {
      case vector =>
        vector match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage.asInstanceOf[LongKeyVectorStorage]
            .getIndices
        }
    }

    val buckets = indices.map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).collect()
    return buckets
  }
}

class StorageBalancePartitioner(bits: Int, numPartitions: Int)
  extends AutoPartitioner(bits, numPartitions) {
  override def getBuckets(data: RDD[Vector]): Array[(Long, Long)] = {
    val indices = data.flatMap {
      case vector =>
        vector match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage.asInstanceOf[LongKeyVectorStorage]
            .getIndices
        }
    }

    val buckets = indices.distinct().map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).collect()
    return buckets
  }
}
