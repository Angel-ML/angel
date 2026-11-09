package com.tencent.angel.spark.ml.graph.data.neighbor

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam
import com.tencent.angel.graph.common.psf.result.GetElementResult
import com.tencent.angel.graph.model.general.get.GeneralGet
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.Param
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrTagElement
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext

import scala.beans.BeanProperty

class NodeNeighborModel(@BeanProperty val param: Param) extends Serializable {

  val neighborsName = "neighbors.data.table"
  var psMatrix: PSMatrix = _


  def getNeighborAttrByte(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrTagElement] = {
    psMatrix.psfGet(new GeneralGet(new LongKeysGetParam(psMatrix.id, nodeIds)))
      .asInstanceOf[GetElementResult].getData.asInstanceOf[Long2ObjectOpenHashMap[NeighborsAttrTagElement]]
  }

  def checkpoint(): Unit = {
    println(s"neighbor table checkpoint now matrixId=${psMatrix.id}")
    psMatrix.checkpoint()
  }
}

object NodeNeighborModel {
  def apply(maxIndex: Long, batchSize: Int, pullBatch: Int, psPartNum: Int, useBalancePartition: Boolean = false): NodeNeighborModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum, useBalancePartition = useBalancePartition)
    new NodeNeighborModel(param)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

}