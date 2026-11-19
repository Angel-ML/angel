package com.tencent.angel.spark.ml.graph.algo.communitydetection.subgraph

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongIntVector, LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.FloatArrayElement
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex.MultiAttrHIndexPSModel
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class SeedPSModel(var inMsgs: PSVector,
                  val psMatrix: PSMatrix) extends Serializable {
  val dim: Long = inMsgs.dimension


  def initAttributes(attrs: RDD[Long], batchSize: Int): Long = {
    attrs.mapPartitions { iter => {
      iter.sliding(batchSize, batchSize).map(pairs => initAttrs(pairs))
    }
    }.reduce(_ + _)
  }

  def initNegNodes(nodes: Array[Long]): Unit = {
    val msgs = VFactory.sparseLongKeyLongVector(dim)
    for (node <- nodes) {
      msgs.set(node, -1) // set neg nodes to -1
    }
    initMsgs(msgs)
  }


  def initAttrs(pairs: Seq[Long]): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(dim)
    for (i <- pairs) {
      msgs.set(i, 1) // the seeds is 1
    }
    initMsgs(msgs)
    msgs.size().toInt
  }


  def initMsgs(msgs: Vector): Unit =
    inMsgs.update(msgs)

  def readMsgs(nodes: Array[Long]): LongLongVector =
    inMsgs.pull(nodes).asInstanceOf[LongLongVector]

  def readAllMsgs(): LongIntVector =
    inMsgs.pull().asInstanceOf[LongIntVector]

  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)

  def checkpoint(iter: Int = 0): Unit = {
    psMatrix.checkpoint(iter)
  }
}

object SeedPSModel {

  def fromMinMax(batchSize: Int, minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): SeedPSModel = {
    val matrix = new MatrixContext("seedNbrs", 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
    matrix.setMaxColNumInBlock((maxId - minId) / psNumPartition)
    matrix.setIndexStart(minId)
    matrix.setIndexEnd(maxId)
    matrix.setColNum(maxId - minId)

    // use balance partition
    if (useBalancePartition)
      LoadBalancePartitioner.partition(data, psNumPartition, matrix)

//    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("seedNbrs").getId
    val psMatrix = PSMatrix.matrix(matrix)
    val matrixId = psMatrix.id

    new SeedPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType), psMatrix)
  }
}