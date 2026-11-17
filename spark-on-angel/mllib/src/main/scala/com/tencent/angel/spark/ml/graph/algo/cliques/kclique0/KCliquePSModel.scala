package com.tencent.angel.spark.ml.graph.algo.cliques.kclique0

import com.tencent.angel.graph.client.initneighbor2.{InitNeighbor => InitLongNeighbor, InitNeighborParam => InitLongNeighborParam}
import com.tencent.angel.graph.client.sampleneighbor2.{SampleNeighbor => SampleLongNeighbor, SampleNeighborParam => SampleLongNeighborParam, SampleNeighborResult => SampleLongNeighborResult}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class KCliquePSModel(msgs: PSMatrix) extends Serializable {
  def initNeighbors(nodeId2Nbrs: Long2ObjectOpenHashMap[Array[Long]]): KCliquePSModel = {
    val func = new InitLongNeighbor(new InitLongNeighborParam(msgs.id, nodeId2Nbrs))
    msgs.asyncPsfUpdate(func).get()
    this
  }

  def readMsgs(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val func = new SampleLongNeighbor(new SampleLongNeighborParam(msgs.id, nodeIds, -1))
    msgs.psfGet(func).asInstanceOf[SampleLongNeighborResult].getNodeIdToNeighbors
  }

  def testPS(neighborsRDD: RDD[KCliqueGraphPartition],
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
    inMC.setName("neighbor-Matrix")
    inMC.setRowNum(1)
    inMC.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    inMC.setColNum(maxId - minId)
    inMC.setMaxColNumInBlock(maxId / psPartitionNum)
    inMC.setValueType(classOf[LongArrayElement])
    inMC.setIndexStart(minId)
    inMC.setIndexEnd(maxId)
    inMC.setPartitionerClass(classOf[ColumnRangePartitioner])

    val inMatrix = PSMatrix.matrix(inMC)

    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psPartitionNum, inMC, balancePartitionPercent)
    }

    new KCliquePSModel(inMatrix)

  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

}
