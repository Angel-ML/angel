package com.tencent.angel.graph.embedding.deepwalkNoWeight

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.general.init.GeneralInit
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement
import com.tencent.angel.graph.model.neighbor.dynamicsimple.psf.{AddNeighbors, AddNeighborsParam, InitNeighbors, InitNeighborsParam}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{IElement, LongArrayElement}
import com.tencent.angel.graph.psf.neighbors.DynamicSimple.SampleWithCount
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.{GetNeighborWithCountParam, GetNeighborsWithCountNoWeight, NeighborsTableElement}
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class DeepWalkPSModel(val edgesPsMatrix: PSMatrix) extends Serializable {
  //push node adjacency list

  def initNodeNeiWithNoWeight(msgs: Long2ObjectOpenHashMap[NeighborsTableElement]): Unit = {
    val nodeIds = new Array[Long](msgs.size())
    val neighborElems = new Array[IElement](msgs.size())
    val iter = msgs.long2ObjectEntrySet().fastIterator()
    var index = 0
    while (iter.hasNext) {
      val i = iter.next()
      nodeIds(index) = i.getLongKey
      neighborElems(index) = i.getValue
      index += 1
    }
    edgesPsMatrix.psfUpdate(new GeneralInit(new LongKeysUpdateParam(edgesPsMatrix.id, nodeIds, neighborElems))).get()
  }

  def addNodeNeiWithNoWeight(data: RDD[(Long, Long)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNodeNeiWithNoWeight(pairs, isAttempt) }
    }.reduce(_ + _)
  }

  def addNodeNeiWithNoWeight(pairs: Seq[(Long, Long)], isAttempt: Boolean): Long = {
    val aggreResult = new Long2ObjectOpenHashMap[ArrayBuffer[Long]]()
    pairs.foreach { case (src, dst) =>
      var neighbors: ArrayBuffer[Long] = aggreResult.get(src)
      if (neighbors == null) {
        neighbors = new ArrayBuffer[Long]()
        neighbors.append(dst)
        aggreResult.put(src, neighbors)
      } else neighbors.append(dst)
    }

    val nodeIds = aggreResult.keySet().toLongArray()
    val neighbors = nodeIds.map(x => new LongArrayElement(aggreResult.get(x).toArray).asInstanceOf[IElement])
    aggreResult.clear()
    val func = new AddNeighbors(new AddNeighborsParam(edgesPsMatrix.id, nodeIds, neighbors, isAttempt))
    edgesPsMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def initNeighborsDegree(data: RDD[(Long, Int)], batchSize: Int, isWeighted: Boolean=false): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => initNeighborsDegree(pairs.toArray, isWeighted) }
    }.reduce(_ + _)
  }

  def initNeighborsDegree(pairs: Array[(Long, Int)], isWeighted: Boolean): Long = {
    val nodeIds = new Array[Long](pairs.length)
    val counts = new Array[Int](pairs.length)
    for (i <- pairs.indices) {
      nodeIds(i) = pairs(i)._1
      counts(i) = pairs(i)._2
    }
    edgesPsMatrix.psfUpdate(new InitNeighbors(new InitNeighborsParam(edgesPsMatrix.id, nodeIds, counts, isWeighted)))
      .get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  //pull node adjacency list
  def getSampledNeighbors(psMatrix: PSMatrix, nodeIds: Array[Long], count: Array[Int], dynamicInitNeighbor: Boolean=false): Long2ObjectOpenHashMap[Array[Long]] = {
    if (dynamicInitNeighbor) {
      psMatrix.psfGet(new SampleWithCount(new GetNeighborWithCountParam(psMatrix.id, nodeIds, count)))
        .asInstanceOf[GetLongsResult].getData
    } else {
      psMatrix.psfGet(new GetNeighborsWithCountNoWeight(new GetNeighborWithCountParam(psMatrix.id, nodeIds, count)))
        .asInstanceOf[GetLongsResult].getData
    }
  }


  def checkpoint(): Unit = {
    edgesPsMatrix.checkpoint()
  }
}

object DeepWalkPSModel {

  def apply(modelContext: ModelContext, data: RDD[Long], useBalancePartition: Boolean,
            balancePartitionPercent: Float, dynamicInitNeighbor: Boolean=false): DeepWalkPSModel = {
    val matrix = if (dynamicInitNeighbor) {
      println("matrix type : DynamicSimpleNeighborElement")
      ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[DynamicSimpleNeighborElement])
    } else {
      println("matrix type : NeighborsTableElement")
      ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsTableElement])
    }

    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition)
      LoadBalancePartitioner.partition(
        data, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)

    val psMatrix = PSMatrix.matrix(matrix)
    new DeepWalkPSModel(psMatrix)
  }

}
