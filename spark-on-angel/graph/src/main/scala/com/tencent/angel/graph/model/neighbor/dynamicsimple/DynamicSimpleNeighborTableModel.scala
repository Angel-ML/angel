package com.tencent.angel.graph.model.neighbor.dynamicsimple

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.GetFloatsResult
import com.tencent.angel.graph.model.ops.CommonOps
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.{IElement, IntFloatArrayPairElement, LongArrayElement, LongFloatArrayPairElement}
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.TimeUnit

class DynamicSimpleNeighborTableModel(modelContex: ModelContext) extends CommonOps with Serializable {

  var neighborMatrix: PSMatrix = _

  def initNeighbors(data: RDD[(Long, Int)], batchSize: Int, isWeighted: Boolean=false): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => initNeighbors(pairs.toArray, isWeighted) }
    }.reduce(_ + _)
  }

  def initNeighbors(pairs: Array[(Long, Int)], isWeighted: Boolean): Long = {
    val startTime = System.currentTimeMillis()
    val nodeIds = new Array[Long](pairs.length)
    val counts = new Array[Int](pairs.length)
    for (i <- pairs.indices) {
      nodeIds(i) = pairs(i)._1
      counts(i) = pairs(i)._2
    }
    neighborMatrix.psfUpdate(new InitNeighbors(new InitNeighborsParam(neighborMatrix.id, nodeIds, counts, isWeighted)))
      .get(1800000, TimeUnit.MILLISECONDS)
    val time = System.currentTimeMillis() - startTime
    println(s"init ${pairs.length} nodes), cost $time ms.")
    pairs.length.toLong
  }

  def addNeighbors(data: RDD[(Long, Long)], batchSize: Int, needReplica: Boolean=false): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNeighbors(pairs, needReplica, isAttempt) }
    }.reduce(_ + _)
  }

  def addNeighbors(pairs: Seq[(Long, Long)], needReplica: Boolean, isAttempt: Boolean): Long = {
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
    val func = new AddNeighbors(new AddNeighborsParam(neighborMatrix.id, nodeIds, neighbors, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    val pushTime = System.currentTimeMillis() - startTime
    println(s"init ${pairs.length} edges (${nodeIds.length} nodes with neighbors), processTime=$aggreTime, pushTime=$pushTime")
    aggreResult.clear()
    if (needReplica) pairs.length.toLong * 2 else pairs.length.toLong
  }

  def addWeightedNeighbors(data: RDD[(Long, Long, Float)], batchSize: Int, needReplica: Boolean=false): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addWeightedNeighbors(pairs, needReplica, isAttempt) }
    }.reduce(_ + _)
  }

  def addWeightedNeighbors(pairs: Seq[(Long, Long, Float)], needReplica: Boolean, isAttempt: Boolean): Long = {
    var startTime = System.currentTimeMillis()
    val aggreResult = new Long2ObjectOpenHashMap[ArrayBuffer[(Long, Float)]]()
    pairs.foreach { case (src, dst, weight) =>
      var neighbors: ArrayBuffer[(Long, Float)] = aggreResult.get(src)
      if (neighbors == null) {
        neighbors = new ArrayBuffer[(Long, Float)]()
        neighbors.append((dst, weight))
        aggreResult.put(src, neighbors)
      } else neighbors.append((dst, weight))
      if (needReplica) {
        var neighbors: ArrayBuffer[(Long, Float)] = aggreResult.get(dst)
        if (neighbors == null) {
          neighbors = new ArrayBuffer[(Long, Float)]()
          neighbors.append((src, weight))
          aggreResult.put(dst, neighbors)
        } else neighbors.append((src, weight))
      }
    }
    val nodeIds = aggreResult.keySet().toLongArray()
    val neighbors = nodeIds.map { nodeId =>
      val data = aggreResult.get(nodeId).toArray
      (new LongFloatArrayPairElement(data.map(_._1), data.map(_._2))).asInstanceOf[IElement]
    }
    aggreResult.clear()
    val aggreTime = System.currentTimeMillis() - startTime

    startTime = System.currentTimeMillis()
    val func = new AddWeightedNeighbors(new AddNeighborsParam(neighborMatrix.id, nodeIds, neighbors, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    val pushTime = System.currentTimeMillis() - startTime
    println(s"add ${pairs.length} edges (${nodeIds.length} nodes with weighted neighbors), processTime=$aggreTime, pushTime=$pushTime")
    aggreResult.clear()
    if (needReplica) pairs.length.toLong * 2 else pairs.length.toLong
  }

  def createAlias(data: RDD[Long], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => createAlias(pairs.toArray) }
    }.reduce(_ + _)
  }

  def createAlias(nodeIds: Array[Long]): Long = {
    // pull weights first
    val start = System.currentTimeMillis()
    var startTime = System.currentTimeMillis()
    val pulledWeights = neighborMatrix.psfGet(new GetWeights(new LongKeysGetParam(neighborMatrix.id, nodeIds)))
      .asInstanceOf[GetFloatsResult].getData
    val pullTime = System.currentTimeMillis() - startTime

    // create alias
    startTime = System.currentTimeMillis()
    val alias = nodeIds.map { node =>
      val weights = pulledWeights.get(node)
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      new IntFloatArrayPairElement(alias, accept).asInstanceOf[IElement]
    }
    val createAliasTime = System.currentTimeMillis() - startTime

    // push to ps
    startTime = System.currentTimeMillis()
    neighborMatrix.psfUpdate(new InitAlias(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, alias)))
      .get(1800000, TimeUnit.MILLISECONDS)
    val pushTime = System.currentTimeMillis() - startTime

    println(s"create and push ${nodeIds.length} nodes alias, cost ${System.currentTimeMillis()-start} ms, " +
      s"pull/create/push cost $pullTime/$createAliasTime/$pushTime ms")

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

  override def init(mc: MatrixContext): Unit = {
    val mc = ModelContextUtils.createMatrixContext(modelContex, RowType.T_ANY_LONGKEY_SPARSE, classOf[DynamicSimpleNeighborElement])
    neighborMatrix = PSMatrix.matrix(mc)
  }

  override def checkpoint(): Unit = {
    neighborMatrix.checkpoint(0)
  }
}
