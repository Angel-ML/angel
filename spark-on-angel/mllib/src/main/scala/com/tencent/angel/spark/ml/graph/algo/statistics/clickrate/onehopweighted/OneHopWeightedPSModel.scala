package com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehopweighted

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.RangePartitioner
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.GetNeighborsAttrs.{GetNeighborsAttrs, GetNeighborsAttrsResult}
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs.{InitNeighborsAttrs, InitNeighborsAttrsParam, NeighborsAttrsElement}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class OneHopWeightedPSModel(var nbrAttrMsgs: PSMatrix) extends Serializable {
  def initNbrAttrMsgs(msgs: Long2ObjectOpenHashMap[NeighborsAttrsElement]): Unit = {
    val func = new InitNeighborsAttrs(new InitNeighborsAttrsParam(nbrAttrMsgs.id, msgs))
    nbrAttrMsgs.asyncPsfUpdate(func).get()
  }
  
  def getNbrAttrMsgs(nodes: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrsElement] = {
    // GetNeighborsAttrs is handled on PS as a GeneralPartGetParam, so use the key-based get param
    nbrAttrMsgs.psfGet(new GetNeighborsAttrs(new LongKeysGetParam(nbrAttrMsgs.id, nodes)))
      .asInstanceOf[GetNeighborsAttrsResult].getNodeIdeToNeighbors
  }
  
}

object OneHopWeightedPSModel{
  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): OneHopWeightedPSModel = {
    val mc = new MatrixContext()
    mc.setName("nbr-attr")
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setColNum(maxId - minId)
    mc.setMaxColNumInBlock(math.max(1L, (maxId - minId + psNumPartition - 1) / psNumPartition))
    mc.setValueType(classOf[NeighborsAttrsElement])
    mc.setIndexEnd(maxId)
    mc.setIndexStart(minId)
  
    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, mc, minId = minId)
    } else {
      // InitNeighborsAttrs/GetNeighborsAttrs split node ids by column range, so the matrix must be
      // range partitioned; the default hash partitioner gives partitions [partId, partId + 1)
      mc.setPartitionerClass(classOf[RangePartitioner])
    }
    
    val psMatrix = PSMatrix.matrix(mc)

    new OneHopWeightedPSModel(psMatrix)
  }
  
}
