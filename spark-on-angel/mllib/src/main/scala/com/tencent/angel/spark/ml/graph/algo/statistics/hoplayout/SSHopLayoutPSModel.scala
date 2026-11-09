package com.tencent.angel.spark.ml.graph.algo.statistics.hoplayout

import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class SSHopLayoutPSModel(var inMsgs: PSVector,
                         var outMsgs: PSVector) extends Serializable {
  val dim: Long = inMsgs.dimension
  
  def initMsgs(msgs: Vector): Unit = {
    inMsgs.update(msgs)
  }
  
  def readMsgs(nodes: Array[Long]): LongIntVector = {
    inMsgs.pull(nodes).asInstanceOf[LongIntVector]
  }
  
  def readAllMsgs(): LongIntVector = {
    inMsgs.pull().asInstanceOf[LongIntVector]
  }
  
  def writeMsgs(msgs: Vector): Unit = {
    outMsgs.update(msgs)
  }
  
  def numMsgs(): Long ={
    VectorUtils.nnz(inMsgs)
  }
  
  def reset(): Unit = {
    val tmp = inMsgs
    inMsgs = outMsgs
    outMsgs = tmp
    outMsgs.reset
  }
}

object SSHopLayoutPSModel {
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long],
                 psPartNum: Int, useBalancePartition: Boolean): SSHopLayoutPSModel = {
    val mc = new MatrixContext()
    mc.setName("sshoplayout")
    mc.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    mc.setValidIndexNum(-1)
    mc.setRowNum(2)
    mc.setIndexStart(minId)
    mc.setIndexEnd(maxId)
    
    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psPartNum, mc)
    }
    
    PSAgentContext.get().getMasterClient.createMatrix(mc, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("sshoplayout").getId
    new SSHopLayoutPSModel(new PSVectorImpl(matrixId, 0, maxId, mc.getRowType),
      new PSVectorImpl(matrixId, 1, maxId, mc.getRowType))
  }
}
