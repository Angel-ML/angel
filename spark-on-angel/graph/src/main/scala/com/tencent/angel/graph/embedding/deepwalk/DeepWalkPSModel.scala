package com.tencent.angel.graph.embedding.deepwalk

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.{GetFloatsResult, GetLongsResult}
import com.tencent.angel.graph.model.general.init.GeneralInit
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{IElement, IntFloatArrayPairElement, LongFloatArrayPairElement}
import com.tencent.angel.graph.psf.neighbors.DynamicSimple.SampleWithCount
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement
import com.tencent.angel.graph.model.neighbor.dynamicsimple.psf.{AddNeighborsParam, AddWeightedNeighbors, GetWeights, InitAlias, InitNeighbors, InitNeighborsParam}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.{GetNeighborWithCountParam, GetNeighborsWithCount, NeighborsAliasTableElement}
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class DeepWalkPSModel(val edgesPsMatrix: PSMatrix) extends Serializable {
  //push node adjacency list

  def initNodeNei(msgs: Long2ObjectOpenHashMap[NeighborsAliasTableElement]): Unit = {
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

  def addNodeNei(data: RDD[(Long, Long, Float)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNodeNei(pairs, isAttempt) }
    }.reduce(_ + _)
  }

  def addNodeNei(pairs: Seq[(Long, Long, Float)], isAttempt: Boolean): Long = {
    val aggreResult = new Long2ObjectOpenHashMap[ArrayBuffer[(Long, Float)]]()
    pairs.foreach { case (src, dst, weight) =>
      var neighbors: ArrayBuffer[(Long, Float)] = aggreResult.get(src)
      if (neighbors == null) {
        neighbors = new ArrayBuffer[(Long, Float)]()
        neighbors.append((dst, weight))
        aggreResult.put(src, neighbors)
      } else neighbors.append((dst, weight))
    }

    val nodeIds = aggreResult.keySet().toLongArray()
    val neighbors = nodeIds.map { nodeId =>
      val data = aggreResult.get(nodeId).toArray
      (new LongFloatArrayPairElement(data.map(_._1), data.map(_._2))).asInstanceOf[IElement]
    }
    aggreResult.clear()
    val func = new AddWeightedNeighbors(new AddNeighborsParam(edgesPsMatrix.id, nodeIds, neighbors, isAttempt))
    edgesPsMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def createAlias(data: RDD[Long], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => createAlias(pairs.toArray) }
    }.reduce(_ + _)
  }

  def createAlias(nodeIds: Array[Long]): Long = {
    // pull weights first
    val pulledWeights = edgesPsMatrix.psfGet(new GetWeights(new LongKeysGetParam(edgesPsMatrix.id, nodeIds)))
      .asInstanceOf[GetFloatsResult].getData

    val alias = nodeIds.map { node =>
      val weights = pulledWeights.get(node)
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      new IntFloatArrayPairElement(alias, accept).asInstanceOf[IElement]
    }

    // push to ps
    edgesPsMatrix.psfUpdate(new InitAlias(new LongKeysUpdateParam(edgesPsMatrix.id, nodeIds, alias)))
      .get(1800000, TimeUnit.MILLISECONDS)

    nodeIds.length.toLong
  }

  def createAliasTable(areaRatio: Array[Float]): (Array[Float], Array[Int]) = {
    val len = areaRatio.length
    val small = ArrayBuffer[Int]()
    val large = ArrayBuffer[Int]()
    val accept = Array.fill(len)(0f)
    val alias = Array.fill(len)(0)

    for (idx <- areaRatio.indices) {
      if (areaRatio(idx) < 1.0) small.append(idx) else large.append(idx)
    }
    while (small.nonEmpty && large.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      val largeIndex = large.remove(large.size - 1)
      accept(smallIndex) = areaRatio(smallIndex)
      alias(smallIndex) = largeIndex
      areaRatio(largeIndex) = areaRatio(largeIndex) - (1 - areaRatio(smallIndex))
      if (areaRatio(largeIndex) < 1.0) small.append(largeIndex) else large.append(largeIndex)
    }
    while (small.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      accept(smallIndex) = 1
    }

    while (large.nonEmpty) {
      val largeIndex = large.remove(large.size - 1)
      accept(largeIndex) = 1
    }
    (accept, alias)
  }

  //pull node adjacency list
  def getSampledNeighbors(psMatrix: PSMatrix, nodeIds: Array[Long], count: Array[Int], dynamicInitNeighbor: Boolean=false): Long2ObjectOpenHashMap[Array[Long]] = {
    if (dynamicInitNeighbor) {
      psMatrix.psfGet(new SampleWithCount(new GetNeighborWithCountParam(psMatrix.id, nodeIds, count)))
        .asInstanceOf[GetLongsResult].getData
    } else {
      psMatrix.psfGet(new GetNeighborsWithCount(new GetNeighborWithCountParam(psMatrix.id, nodeIds, count)))
        .asInstanceOf[GetLongsResult].getData
    }
  }

  def checkpoint(): Unit = {
    edgesPsMatrix.checkpoint()
  }
}

object DeepWalkPSModel {
  def apply(modelContext: ModelContext, data: RDD[Long],
            useBalancePartition: Boolean, balancePartitionPercent: Float, dynamicInitNeighbor: Boolean=false): DeepWalkPSModel = {
    val matrix = if (dynamicInitNeighbor)
      ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[DynamicSimpleNeighborElement])
    else ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[NeighborsAliasTableElement])
    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition)
      LoadBalancePartitioner.partition(
        data, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)

    val psMatrix = PSMatrix.matrix(matrix)
    new DeepWalkPSModel(psMatrix)

  }
}