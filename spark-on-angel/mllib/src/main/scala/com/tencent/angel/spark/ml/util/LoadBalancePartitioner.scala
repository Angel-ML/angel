package com.tencent.angel.spark.ml.util

import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import com.tencent.angel.ml.math2.vector.{LongDummyVector, LongKeyVector, Vector}
import org.apache.spark.rdd.RDD

class LoadBalancePartitioner(bits: Int, numPartitions: Int) {

  def partitionMatrix(data: RDD[Vector], ctx: MatrixContext) = {
    val indices = data.flatMap {
      case vector =>
        vector match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage.asInstanceOf[LongKeyVectorStorage]
            .getIndices
        }}

    val buckets = indices.map(f => f / (1L << bits))
      .map(f => (f, 1L)).reduceByKey(_ + _).collect()

    val sorted = buckets.sortBy(f => f._1)
    val sum = sorted.map(f => f._2).sum
    val per = sum / numPartitions

    var start = ctx.getIndexStart
    val end = ctx.getIndexEnd

    val rowNum = ctx.getRowNum

      var current = 0L
      val size = sorted.size
      for (i <- 0 until size) {
        if (current > per) {
          current = 0L
          val part = new PartContext(0, rowNum, start, sorted(i)._1, 0)
          ctx.addPart(part)
          start = sorted(i)._1
        }
        current += sorted(i)._2
    }

    ctx.addPart(new PartContext(0, rowNum, start, end, 0))
    println(s"split matrix ${ctx.getName} into ${ctx.getParts.size()} partitions")
  }

}
