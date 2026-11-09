package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition


import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntLongVector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.graph.utils.{BatchIter, GraphIO}
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


class DeIndex extends Serializable {

  import DeIndex._

  private var int2long: PSVector = _
  private var numPSPartition: Int = -1
  private var nodesMapPath: String = _

  def index(nodesMapPath: String, numPSPartition: Int, batchSize: Int = 1000000): Unit = {

    this.numPSPartition = numPSPartition
    this.nodesMapPath = nodesMapPath

    val long2IntMap = GraphIO.load(nodesMapPath, isWeighted = false, sep = "\t")
      .select("src", "dst").rdd
      .map(row => (row.getLong(0), row.getLong(1).toInt))

    long2IntMap.persist(StorageLevel.DISK_ONLY)

    // keys could be negative int nums
    val keys = long2IntMap.map(_._2)
    val minId = keys.min().toLong
    val maxId = (keys.max() + 1).toLong
    val ctx = new MatrixContext(INT2LONG, 1, minId, maxId)
    ctx.setRowType(RowType.T_LONG_SPARSE)
    ctx.setValidIndexNum(-1)

    PSAgentContext.get().getMasterClient.createMatrix(ctx, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix(INT2LONG).getId

    this.int2long = new PSVectorImpl(matrixId, 0, maxId - minId, ctx.getRowType)

    long2IntMap.foreachPartition { iter =>
      BatchIter(iter, batchSize).foreach { batch =>
        val (value, key) = batch.unzip
        val int2longVec = VFactory.sparseLongVector(Int.MaxValue, key, value)
        int2long.update(int2longVec)
      }
    }
    long2IntMap.unpersist(false)
  }

  def decodeEdge(rdd: RDD[(Int, Int)], batchSize: Int): RDD[(Long, Long)] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.flatMap { case (src, dst) => Iterator(src, dst) }.distinct
        val map = this.int2long.pull(keys).asInstanceOf[IntLongVector]
        batch.map { case (src, dst) =>
          (map.get(src), map.get(dst))
        }.toIterator
      }
    }
  }

  def decodeEdge[U: ClassTag](rdd: RDD[(Int, Int, U)], batchSize: Int): RDD[(Long, Long, U)] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.flatMap { case (src, dst, _) => Iterator(src, dst) }.distinct
        val map = this.int2long.pull(keys).asInstanceOf[IntLongVector]
        batch.map { case (src, dst, other) =>
          (map.get(src), map.get(dst), other)
        }.toIterator
      }
    }
  }

  def decodeNode(rdd: RDD[Int], batchSize: Int): RDD[Long] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.distinct
        val map = this.int2long.pull(keys).asInstanceOf[IntLongVector]
        batch.map { node => map.get(node)
        }.toIterator
      }
    }
  }

  def decodeNode[U: ClassTag](rdd: RDD[(Int, U)], batchSize: Int): RDD[(Long, U)] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.map(_._1).distinct
        val map = this.int2long.pull(keys).asInstanceOf[IntLongVector]
        batch.map { case (node, other) =>
          (map.get(node), other)
        }.toIterator
      }
    }
  }

}

object DeIndex {
  val INT2LONG = "int2long"
}
