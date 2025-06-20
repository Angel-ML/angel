package com.tencent.angel.graph.kclique

import com.tencent.angel.graph.Param
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.graph.utils.element.NeighborTable
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty
import scala.reflect.ClassTag


class KCliquePSModel(@BeanProperty val param: Param) extends Serializable {
  val neighborTableName = param.matrixName
  var psMatrix: PSMatrix = _
  var inRowId: Int = 0
  var outRowId: Int = 1

  // init ps and push 2cliqie info into ps
  def init2Clique[ED: ClassTag](data: RDD[KCliqueGraphPartition[ED]]): this.type = {
    val modelContext = new ModelContext(param.psPartNum, param.minIndex, param.maxIndex + 1, -1,
      "KClique", SparkContext.getOrCreate().hadoopConfiguration)
    val mc: MatrixContext = ModelContextUtils.createMatrixContext(modelContext, neighborTableName,
      RowType.T_ANY_LONGKEY_SPARSE, classOf[LongArrayElement], 2)

    if (!modelContext.isUseHashPartition) {
      LoadBalancePartitioner.partition(data.flatMap(p => p.srcIds), modelContext.getMaxNodeId, modelContext.getPartitionNum, mc)
    }
    psMatrix = PSMatrix.matrix(mc)
    inRowId = 0
    outRowId = 1

    data.foreach { part =>
      val size = part.size
      var pos = 0
      while (pos < size) {
        psInit2Clique(psMatrix, part.takeBatch(pos, param.batchSize))
        pos += param.batchSize
      }
    }
    this
  }

  // push 2clique info to nodes,
  // each node store clique with id bigger than its own.
  def psInit2Clique[ED: ClassTag](psMatrix: PSMatrix, pairs: Array[NeighborTable[ED]]): this.type = {
    val nodeIdToNeighbors = new Long2ObjectOpenHashMap[Array[Long]](pairs.length)
    pairs.foreach { item =>
      require(item.srcId < param.maxIndex, s"${item.srcId} exceeds the maximal node index ${param.maxIndex}")
      nodeIdToNeighbors.put(item.srcId, item.neighborIds.filter(nbr => nbr > item.srcId))
    }
    val func = new LongObjectUF(new KCliqueUpdateParam(psMatrix.id, inRowId, nodeIdToNeighbors))
    psMatrix.asyncPsfUpdate(func).get()
    nodeIdToNeighbors.clear()
    println(s"init ${pairs.length} clique nodes, count ${pairs.length} 2-cliques")
    this
  }

  def readMsgs(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    psGetClique(nodeIds, inRowId, psMatrix)
  }

  def psGetClique(nodeIds: Array[Long], rowId: Int, psMatrix: PSMatrix = psMatrix): Long2ObjectOpenHashMap[Array[Long]] = {
    val func = new SampleNodeInfo(new KCliqueGetParam(psMatrix.id, rowId, nodeIds, -1))
    psMatrix.asyncPsfGet(func).get()
      .asInstanceOf[SampleNodeInfoResult].getNodeIdToNeighbors
  }

  def writeMsgs[ED: ClassTag](pairs: Array[NeighborTable[ED]]): this.type = {
    psPushClique(psMatrix, pairs, outRowId)
  }

  // reset ps and push new info
  def psPushClique[ED: ClassTag](psMatrix: PSMatrix = psMatrix, pairs: Array[NeighborTable[ED]], rowId: Int): this.type = {
    val nodeIdToNeighbors = new Long2ObjectOpenHashMap[Array[Long]](pairs.length)
    pairs.foreach { item =>
      require(item.srcId < param.maxIndex, s"${item.srcId} exceeds the maximal node index ${param.maxIndex}")
      nodeIdToNeighbors.put(item.srcId, item.neighborIds)
    }
    val func = new LongObjectUF(new KCliqueUpdateParam(psMatrix.id, rowId, nodeIdToNeighbors))

    psMatrix.asyncPsfUpdate(func).get()
    nodeIdToNeighbors.clear()
    this
  }

  def resetMsgs(): Unit = {
    val tmp = inRowId
    inRowId = outRowId
    outRowId = tmp
    psMatrix.reset(outRowId)
  }

  def testPS[ED: ClassTag](neighborsRDD: RDD[KCliqueGraphPartition[ED]],
                           num: Int = 10): KCliquePSModel = {
    val correct = neighborsRDD.map { part =>
      part.testPS(this, num)
    }.reduce(_ && _)
    assert(correct, "neighbor table is wrong")
    this
  }

}

object KCliquePSModel {
  def apply(maxIndex: Long, batchSize: Int, pullBatch: Int, psPartNum: Int): KCliquePSModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum)
    new KCliquePSModel(param)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }
}
