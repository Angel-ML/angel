package com.tencent.angel.graph.model.neighbor.dynamic

import java.util.concurrent.TimeUnit

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.neighbor.dynamic.psf.get.{GetNeighbor, GetNodes, GetNodesParam, GetNodesResult}
import com.tencent.angel.graph.model.neighbor.dynamic.psf.init.GetSort.GetSortParam
import com.tencent.angel.graph.model.neighbor.dynamic.psf.init.GetSortByKeys.GetSortByKeysParam
import com.tencent.angel.graph.model.neighbor.dynamic.psf.init.{GetSort, GetSortByKeys, InitDynamicNbrs}
import com.tencent.angel.graph.model.ops.CommonOps
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class DynamicNeighborModel(modelContex: ModelContext) extends CommonOps with Serializable {
  var neighborMatrix: PSMatrix = _

  override def init(mc: MatrixContext): Unit = {
    val mc = ModelContextUtils.createMatrixContext(
      modelContex, RowType.T_ANY_LONGKEY_SPARSE, classOf[DynamicNeighborElement])
    mc.setParts(mc.getParts)
    neighborMatrix = PSMatrix.matrix(mc)
  }

  override def checkpoint(): Unit = neighborMatrix.checkpoint()

  def initNeighbors(data: RDD[(Long, Long)], batchSize: Int, needReplica: Boolean=false): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => initNeighbors(pairs, needReplica) }
    }.reduce(_ + _)
  }

  def initNeighbors(pairs: Seq[(Long, Long)], needReplica: Boolean): Long = {
    var startTime = System.currentTimeMillis()
    val aggreResult = new Long2ObjectOpenHashMap[ArrayBuffer[Long]]()
    pairs.foreach { case (src, dst) =>
      var neighbors: ArrayBuffer[Long] = aggreResult.get(src)
      if (neighbors == null) {
        neighbors = new ArrayBuffer[Long]()
        neighbors.append(dst)
        aggreResult.put(src, neighbors)
      } else neighbors.append(dst)
      if (needReplica) {
        var neighbors: ArrayBuffer[Long] = aggreResult.get(dst)
        if (neighbors == null) {
          neighbors = new ArrayBuffer[Long]()
          neighbors.append(src)
          aggreResult.put(dst, neighbors)
        } else neighbors.append(src)
      }
    }
    val nodeIds = aggreResult.keySet().toLongArray()
    val neighbors = nodeIds.map(x => new LongArrayElement(aggreResult.get(x).toArray).asInstanceOf[IElement])
    aggreResult.clear()
    val aggreTime = System.currentTimeMillis() - startTime

    startTime = System.currentTimeMillis()
    val func = new InitDynamicNbrs(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, neighbors))
    neighborMatrix.psfUpdate(func).get(600000, TimeUnit.MILLISECONDS)
    val pushTime = System.currentTimeMillis() - startTime
    println(s"init ${pairs.length} edges (${nodeIds.length} nodes with neighbors), processTime=$aggreTime, pushTime=$pushTime")
    aggreResult.clear()
    pairs.length.toLong
  }

  def trans(): Unit = {
    val func = new GetSort(new GetSortParam(neighborMatrix.id, false))
    neighborMatrix.asyncPsfUpdate(func).get(600000, TimeUnit.MILLISECONDS)
  }

  def trans(nodes: RDD[Long], batchSize: Int): Long = {
    nodes.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map{ batchNodes =>
        trans(batchNodes.toArray)
      }
    }.reduce(_ + _)
  }

  def trans(nodes: Array[Long]): Long = {
    val before = System.currentTimeMillis()
    val func = new GetSortByKeys(new GetSortByKeysParam(neighborMatrix.id, nodes))
    neighborMatrix.asyncPsfUpdate(func).get(600000, TimeUnit.MILLISECONDS)
    println(s"transformed ${nodes.length} nodes on ps, cost ${System.currentTimeMillis() - before} ms.")
    nodes.length
  }

  def getNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    neighborMatrix.asyncPsfGet(new GetNeighbor(
      new LongKeysGetParam(neighborMatrix.id, nodeIds))).get(600000, TimeUnit.MILLISECONDS).asInstanceOf[GetLongsResult].getData

  }

  def getNodes(psPartitionNum: Int): RDD[Long] = {
    val psPartIds = SparkContext.getOrCreate().parallelize(0 until psPartitionNum, psPartitionNum)
    psPartIds.flatMap { psPartId =>
      val func = new GetNodes(new GetNodesParam(neighborMatrix.id, psPartId))
      neighborMatrix.asyncPsfGet(func).get(600000, TimeUnit.MILLISECONDS).asInstanceOf[GetNodesResult].getNodes
    }
  }

}
