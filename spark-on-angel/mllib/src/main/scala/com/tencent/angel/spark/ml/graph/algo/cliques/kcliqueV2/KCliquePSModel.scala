package com.tencent.angel.spark.ml.graph.algo.cliques.kcliqueV2

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.VertexId
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class KCliquePSModel(var inMsgs: PSMatrix,
                     var outMsgs: PSMatrix) extends Serializable {
  // reset ps and push new info
  def psPushClique(psMatrix: PSMatrix, msgs: Long2ObjectOpenHashMap[Array[VertexId]]): Unit = {
    val func = new LongObjectUF(new KCliqueUpdateParam(psMatrix.id, msgs))
    psMatrix.asyncPsfUpdate(func).get()
  }
  
  def psGetClique(psMatrix: PSMatrix, nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val func = new SampleNodeInfo(new KCliqueGetParam(psMatrix.id, nodeIds, -1))
    psMatrix.psfGet(func)
      .asInstanceOf[SampleNodeInfoResult].getNodeIdToNeighbors
  }
  
  def readMsgs(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    psGetClique(inMsgs, nodeIds)
  }
  
  def writeMsgs[ED: ClassTag](msgs: Long2ObjectOpenHashMap[Array[VertexId]]): Unit = {
    psPushClique(outMsgs, msgs)
  }
  
  def resetMsgs(): Unit ={
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset()
  }
  
  def testPS[ED: ClassTag](neighborsRDD: RDD[KCliqueGraphPartition],
                           num: Int = 10): KCliquePSModel = {
    val correct = neighborsRDD.map { part =>
      part.testPS(this, num)
    }.reduce(_ && _)
    assert(correct, "neighbor table is wrong")
    this
  }
  
}

object KCliquePSModel {
  def fromMinMax(minId: Long, maxId: Long,
                 data: RDD[Long], psPartitionNum: Int,
                 useBalancePartition: Boolean,
                 balancePartitionPercent: Float): KCliquePSModel = {
    val inMC = new MatrixContext()
    inMC.setName("inMatrix")
    inMC.setRowNum(1)
    inMC.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    inMC.setColNum(maxId - minId)
    inMC.setMaxColNumInBlock(maxId / psPartitionNum)
    inMC.setValueType(classOf[LongArrayElement])
    inMC.setIndexStart(minId)
    inMC.setIndexEnd(maxId)
    inMC.setPartitionerClass(classOf[ColumnRangePartitioner])
    
    val inMatrix = PSMatrix.matrix(inMC)
    
    val outMC = new MatrixContext()
    outMC.setName("outMatrix")
    outMC.setRowNum(1)
    outMC.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    outMC.setColNum(maxId - minId)
    outMC.setMaxColNumInBlock(maxId / psPartitionNum)
    outMC.setValueType(classOf[LongArrayElement])
    outMC.setIndexStart(minId)
    outMC.setIndexEnd(maxId)
    outMC.setPartitionerClass(classOf[ColumnRangePartitioner])
    
    val outMatrix = PSMatrix.matrix(outMC)
  
    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psPartitionNum, inMC, balancePartitionPercent)
      LoadBalancePartitioner.partition(data, maxId, psPartitionNum, outMC, balancePartitionPercent)
    }
    
    new KCliquePSModel(inMatrix, outMatrix)
    
  }
  
  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }
  
}
