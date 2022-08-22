package com.tencent.angel.graph.embedding.struct2vec


import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.general.init.GeneralInit
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.{GetNeighborWithCountParam, GetNeighborsWithCount, NeighborsAliasTableElement}


class Struct2vecPSModel(val edgesPsMatrix: PSMatrix) extends Serializable  {
  //分布式获取节点的邻接表

  def initNodeNei(msgs: Long2ObjectOpenHashMap[NeighborsAliasTableElement]): Unit = {
    //msgs是储存图节点的HashMap
    val nodeIds = new Array[Long](msgs.size())
    val neighborElems = new Array[IElement](msgs.size()) //邻居节点
    val iter = msgs.long2ObjectEntrySet().fastIterator() //迭代器
    var index = 0
    //遍历迭代器，构建节点的id数组和邻居数组
    while (iter.hasNext) {
      val i = iter.next()
      nodeIds(index) = i.getLongKey
      neighborElems(index) = i.getValue
      index += 1
    }
    //用构建的节点数组和邻居数组初始化边的矩阵
    edgesPsMatrix.psfUpdate(new GeneralInit(new LongKeysUpdateParam(edgesPsMatrix.id, nodeIds, neighborElems))).get()
  }

  //取节点测试
  def getSampledNeighbors(psMatrix: PSMatrix, nodeIds: Array[Long], count: Array[Int]): Long2ObjectOpenHashMap[Array[Long]] = {
    psMatrix.psfGet(
      new GetNeighborsWithCount(
        new GetNeighborWithCountParam(psMatrix.id, nodeIds, count))).asInstanceOf[GetLongsResult].getData
  }

  def checkpoint(): Unit = {
    edgesPsMatrix.checkpoint()
  }

}

//伴生对象
object Struct2vecPSModel {
  def apply(modelContext: ModelContext, data: RDD[Long],
            useBalancePartition: Boolean, balancePartitionPercent: Float): Struct2vecPSModel= {
    //创建上下文
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsAliasTableElement])

    // 调用后删除上下文
    if (!modelContext.isUseHashPartition && useBalancePartition)
      LoadBalancePartitioner.partition(
        data, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)

    val psMatrix = PSMatrix.matrix(matrix)
    new DeepWalkPSModel(psMatrix)

  }
}
