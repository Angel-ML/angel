package com.tencent.angel.graph.utils.reindex

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntIntVector, IntLongVector, LongIntVector}
import com.tencent.angel.ml.matrix.{MatrixContext, PartContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.collection.JavaConversions._


class NodeIndexer extends Serializable {

  import NodeIndexer._

  private var long2int: PSVector = _
  private var int2long: PSVector = _
  private var numPSPartition: Int = -1
  private var numNodes: Int = -1

  def getNumOfNodes: Int = {
    assert(numNodes > 0, "number of nodes should greater than zero")
    numNodes
  }

  def index(numPSPartition: Int, minId: Long, maxId: Long, nodes: RDD[Long], nodesNum: Int, batchSize: Int = 1000000): Unit = {

    this.numPSPartition = numPSPartition
    nodes.persist(StorageLevel.DISK_ONLY)

    val ctx = new MatrixContext(LONG2INT, 1, -1)
    ctx.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    this.long2int = new PSVectorImpl(PSMatrixUtils.createPSMatrix(ctx), 0, maxId, RowType.T_INT_SPARSE_LONGKEY)

    val nodeIndex = nodes.map((_, null)).sortByKey().zipWithIndex().map(x => (x._1._1, x._2))
    this.numNodes = nodesNum

    nodes.unpersist(false)

    val ctx2 = new MatrixContext(INT2LONG, 1, this.numNodes)
    ctx2.setRowType(RowType.T_LONG_DENSE)

    nodeIndex.mapPartitions { iter =>
      val first = iter.next()._2
      val last = iter.toArray.last._2
      Iterator.single((first, last))
    }.collect().foreach { case (start, end) =>
      ctx2.addPart(new PartContext(0, 1, start, end + 1L, (end - start).toInt))
    }

    this.int2long = new PSVectorImpl(PSMatrixUtils.createPSMatrix(ctx2), 0, nodesNum + 1, RowType.T_LONG_DENSE)

    nodeIndex.foreachPartition { iter =>
      BatchIter(iter, batchSize).foreach { batch =>
        val (key, value) = batch.unzip
        val intValues = value.map(_.toInt)
        val long2intVec = VFactory.sparseLongKeyIntVector(Long.MaxValue, key, intValues)
        val int2longVec = VFactory.sparseLongVector(this.numNodes, intValues, key)
        long2int.update(long2intVec)
        int2long.update(int2longVec)
      }
    }
    nodeIndex.unpersist(false)
  }


  def encode[U: ClassTag](rdd: RDD[(Long, Long, U)], batchSize: Int): RDD[(Int, Int, U)] = {
    rdd.mapPartitions { case iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.flatMap { case (src, dst, _) => Iterator(src, dst) }.distinct
        val map = this.long2int.pull(keys).asInstanceOf[LongIntVector]
        batch.map { case (src, dst, other) =>
          (map.get(src), map.get(dst), other)
        }.toIterator
      }
    }
  }

  def decode[U: ClassTag](rdd: RDD[(Int, Int, U)], batchSize: Int): RDD[(Long, Long, U)] = {
    rdd.mapPartitions { case iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.flatMap { case (src, dst, _) => Iterator(src, dst) }.distinct
        val map = this.int2long.pull(keys).asInstanceOf[IntLongVector]
        batch.map { case (src, dst, other) =>
          (map.get(src), map.get(dst), other)
        }.toIterator
      }
    }
  }

  def decode2[U: ClassTag](rdd: RDD[(Int, U)], batchSize: Int): RDD[(Long, U)] = {
    rdd.mapPartitions { case iter =>
      BatchIter(iter, batchSize).flatMap { batch =>
        val keys = batch.map(_._1).distinct
        val map = this.int2long.pull(keys).asInstanceOf[IntLongVector]
        batch.map { case (node, other) =>
          (map.get(node), other)
        }.toIterator
      }
    }
  }

  def decodeInt2IntPSVector(ps: PSVector): RDD[(Long, Long)] = {
    val sc = SparkContext.getOrCreate()
    val master = PSAgentContext.get().getMasterClient
    val partitions = master.getMatrix(INT2LONG)
      .getPartitionMetas.map { case (_, p) =>
      (p.getStartCol.toInt, p.getEndCol.toInt)
    }.toSeq

    sc.parallelize(partitions, this.numPSPartition).flatMap { case (start, end) =>
      val intKeys = Array.range(start, end)
      val intValues = ps.pull(intKeys.clone()).asInstanceOf[IntIntVector].get(intKeys)
      val map = int2long.pull(intKeys ++ intValues).asInstanceOf[IntLongVector]
      map.get(intKeys).zip(map.get(intValues))
    }
  }

}

object NodeIndexer {
  val LONG2INT = "long2int"
  val INT2LONG = "int2long"
}
