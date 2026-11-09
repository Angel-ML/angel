package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


class ReIndex() extends Serializable {

  import ReIndex._

  private var long2int: PSVector = _
  private var numPSPartition: Int = -1
  private var numNodes: Long = -1

  def getNumOfNodes: Long = {
    assert(numNodes > 0, "number of nodes should greater than zero")
    numNodes
  }

  def index(numPSPartition: Int, minId: Long, maxId: Long, nodes: RDD[Long], nodesNum: Long, batchSize: Int = 1000000, long2intPath: String): Unit = {

    this.numPSPartition = numPSPartition

    val ctx = new MatrixContext(LONG2INT, 1, minId, maxId)
    ctx.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    this.long2int = new PSVectorImpl(PSMatrixUtils.createPSMatrix(ctx), 0, maxId, RowType.T_INT_SPARSE_LONGKEY)

    val intRange = Int.MaxValue.toLong - Int.MinValue.toLong + 1l
    val exceedNum = nodesNum - intRange
    assert(exceedNum <= 0, s"nodesNum exceeds intRange: $nodesNum vs $intRange, could not trans nodeId to int type.")
    val offset = if (nodesNum > Int.MaxValue) nodesNum - Int.MaxValue else 0
    val nodeIndex = nodes.map((_, null)).sortByKey().zipWithIndex().map(x => (x._1._1, (x._2 - offset).toInt)).persist(StorageLevel.DISK_ONLY)

    // save nodeIndex
    val schema = StructType(Seq(
      StructField(s"longNode", LongType, nullable = false),
      StructField(s"intNode", IntegerType, nullable = false)
    ))
    GraphIO.save(SparkSession.builder().getOrCreate().createDataFrame(nodeIndex.map(x => Row(x._1, x._2)), schema), long2intPath)
    this.numNodes = nodesNum
    nodes.unpersist()

    nodeIndex.foreachPartition { iter =>
      BatchIter(iter, batchSize).foreach { batch =>
        val (key, value) = batch.unzip
        val long2intVec = VFactory.sparseLongKeyIntVector(Long.MaxValue, key, value)
        long2int.update(long2intVec)
      }
    }
    nodeIndex.unpersist()
  }


  def encode[U: ClassTag](rdd: RDD[(Long, Long, U)], batchSize: Int, partitionNum: Int): RDD[(Int, Int, U)] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.flatMap { case (src, dst, _) => Iterator(src, dst) }.distinct
        val map = this.long2int.pull(keys).asInstanceOf[LongIntVector]
        batch.map { case (src, dst, other) =>
          (map.get(src), map.get(dst), other)
        }.toIterator
      }
    }
  }

  def encode(rdd: RDD[(Long, Long)], batchSize: Int, partitionNum: Int): RDD[(Int, Int)] = {
    rdd.mapPartitions { iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.flatMap { case (src, dst) => Iterator(src, dst) }.distinct
        val map = this.long2int.pull(keys).asInstanceOf[LongIntVector]
        batch.map { case (src, dst) =>
          (map.get(src), map.get(dst))
        }.toIterator
      }
    }
  }

}

object ReIndex {
  val LONG2INT = "long2int"
}
