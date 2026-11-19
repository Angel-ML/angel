package com.tencent.angel.spark.ml.graph.algo.embedding.metaPath2vec

import it.unimi.dsi.fastutil.ints.IntArrayList
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.{LongKeysGetParam, LongKeysUpdateParam}
import com.tencent.angel.graph.common.psf.result.{GetFloatsResult, GetIntsResult, GetLongsResult}
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement
import com.tencent.angel.graph.model.neighbor.dynamicsimple.psf
import com.tencent.angel.graph.model.neighbor.dynamicsimple.psf.{AddNeighbors, AddNeighborsParam, AddWeightedNeighbors, GetWeights, InitAlias, InitNeighborsParam}
import com.tencent.angel.graph.model.neighbor.dynamicsimplewithtype.psf.{AddNeighborsWithType, AddWeightedNeighborsWithType, GetIndptr, GetTypes}
import com.tencent.angel.graph.model.neighbor.dynamicsimplewithtype.psf.sample.DynamicSampleWithType
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.model.neighbor.simplewithtype.TypeNeighborElement
import com.tencent.angel.graph.model.neighbor.simplewithtype.psf.getType.GetType
import com.tencent.angel.graph.model.neighbor.simplewithtype.psf.sample.{Sample, SampleParam}
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ps.storage.vector.element.{IElement, IntFloatArrayPairElement, LongArrayElement, LongFloatArrayPairElement, LongIntIntArrayPairElement, LongIntIntFloatArrayPairElement}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MetaPath2VecPSModel(nbrModelContext: ModelContext,
                          tagModelContext: ModelContext = null,
                          dynamicInitNeighbor: Boolean = false) extends Serializable {
  var tagMatrix: PSMatrix = _
  var neighborMatrix: PSMatrix = _
  var tagVector: PSVector = _

  def init(): Unit = {
    val neighborMc = if (dynamicInitNeighbor)
      ModelContextUtils.createMatrixContext(nbrModelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[DynamicSimpleNeighborElement])
    else ModelContextUtils.createMatrixContext(nbrModelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[TypeNeighborElement])
    neighborMatrix = PSMatrix.matrix(neighborMc)

    if (tagModelContext != null) {
      val tagMc = ModelContextUtils.createMatrixContext(tagModelContext, RowType.T_INT_SPARSE_LONGKEY)
      tagMatrix = PSMatrix.matrix(tagMc)
      tagVector = new PSVectorImpl(tagMatrix.id, 0, tagModelContext.getMaxNodeId, tagMc.getRowType)
    }
  }

  def initMsgs(arr: Array[(Long, Int)], batchSize: Int): Iterator[Int] = {
    arr.sliding(batchSize, batchSize).map { iter =>
      val msgs = VFactory.sparseLongKeyIntVector(iter.length)
      for (idx <- iter.indices) {
        val (key, tag) = iter(idx)
        msgs.set(key, tag)
      }
      initMsgs(msgs)
      msgs.clear()
      iter.length
    }
  }

  def initMsgs(msgs: Vector): Unit =
    tagVector.update(msgs)

  def numMsgs(): Long = VectorUtils.nnz(tagVector)

  def readMsgs(nodes: Array[Long]): LongIntVector =
    tagVector.pull(nodes).asInstanceOf[LongIntVector]

  def initNeighbors(nodeIds: Array[Long], data: Array[IElement]): Unit = {
    val func = new InitNeighbors(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, data))
    neighborMatrix.asyncPsfUpdate(func).get()
    println(s"init ${nodeIds.length} type neighbors.")
  }

  def getTypes(iter: Array[Long], metaPath: Set[Int]) = {
    val pulled = readMsgs(iter)
    // make sure nodes' types is in metaPath
    iter.flatMap { x =>
      val t = pulled.get(x)
      if (metaPath.contains(t)) Iterator.single(x, t) else Iterator.empty
    }.toIterator
  }

  def getTypes(iter: Array[Long]) = {
    val pulled = readMsgs(iter)
    // make sure nodes' types is in metaPath
    iter.flatMap { x =>
      val t = pulled.get(x)
      Iterator.single(x, t)
    }.toIterator
  }

  // for edge type meta-path, return edge types for each node
  def getNodeEdgeTypes(nodes: Array[Long]) = {
    val pulled = neighborMatrix.psfGet(new GetType(new LongKeysGetParam(neighborMatrix.id, nodes)))
      .asInstanceOf[GetIntsResult].getData
    nodes.map(x => (x, pulled.get(x))).filter(_._2.nonEmpty)
      .flatMap { case (node, types) => types.map(t => (node, t)) }
      .toIterator
  }

  def sample(nodes: Array[Long], sampleType: Int, sampleByType: Boolean, dynamicInitNeighbor: Boolean=false) = {
    val pulled = if (dynamicInitNeighbor) {
      neighborMatrix.psfGet(new DynamicSampleWithType(new SampleParam(neighborMatrix.id, nodes, sampleType, sampleByType)))
        .asInstanceOf[GetLongsResult].getData
    } else {
      neighborMatrix.psfGet(new Sample(new SampleParam(neighborMatrix.id, nodes, sampleType, sampleByType)))
        .asInstanceOf[GetLongsResult].getData
    }
    val out = new Long2ObjectOpenHashMap[Iterator[Long]](pulled.size())
    val iter = pulled.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val next = iter.next()
      val data = next.getValue.toIterator
      out.put(next.getLongKey, data)
    }
    out
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
    neighborMatrix.psfUpdate(new psf.InitNeighbors(new InitNeighborsParam(neighborMatrix.id, nodeIds, counts, isWeighted)))
      .get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def initNeighborsDegreeWithType(data: RDD[(Long, Array[(Int, Int)])], batchSize: Int, isWeighted: Boolean=false): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => initNeighborsDegreeWithType(pairs.toArray, isWeighted) }
    }.reduce(_ + _)
  }

  def initNeighborsDegreeWithType(pairs: Array[(Long, Array[(Int, Int)])], isWeighted: Boolean): Long = {
    val nodeIds = new Array[Long](pairs.length)
    val data = new Array[IElement](pairs.length)

    for (i <- pairs.indices) {
      var counts = 0
      val types = new IntArrayList()
      val indptr = new IntArrayList()
      val indexes = new IntArrayList()

      indptr.add(0)
      indexes.add(0)
      types.add(pairs(i)._2(0)._1)
      counts += pairs(i)._2(0)._2
      for (j <- 1 until pairs(i)._2.length) {
        indptr.add(counts)
        indexes.add(counts)
        types.add(pairs(i)._2(j)._1)
        counts += pairs(i)._2(j)._2
      }
      indptr.add(counts)
      nodeIds(i) = pairs(i)._1
      data(i) = if (isWeighted) {
        new DynamicSimpleNeighborElement(new Array[Long](counts), new Array[Float](counts), types.toIntArray(), indptr.toIntArray(), indexes.toIntArray())
      } else {
        new DynamicSimpleNeighborElement(new Array[Long](counts), types.toIntArray(), indptr.toIntArray(), indexes.toIntArray())
      }
    }
    neighborMatrix.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, data)))
      .get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def addNodeNeiWithNoWeightWithEdgeType(data: RDD[(Long, Long, Int)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNodeNeiWithNoWeightWithEdgeType(pairs.toArray, isAttempt) }
    }.reduce(_ + _)
  }

  def addNodeNeiWithNoWeightWithEdgeType(pairs: Array[(Long, Long, Int)], isAttempt: Boolean): Long = {
    val new_pairs = pairs.groupBy(r => r._1)

    val nodeIds = new Array[Long](new_pairs.size)
    val data = new Array[IElement](new_pairs.size)

    var idx = 0
    new_pairs.foreach { case (src, nbrs) =>
      val indptr = new IntArrayList()
      val typeList = new IntArrayList()
      val (nbrs_, tags) = nbrs.map(x => (x._2, x._3)).sortWith(_._2 < _._2).unzip
      createTypeNeighbors(indptr, typeList, tags, nbrs_)
      nodeIds(idx) = src
      data(idx) = new LongIntIntArrayPairElement(nbrs_, typeList.toIntArray(), indptr.toIntArray())
      idx += 1
    }

    val func = new AddNeighborsWithType(new AddNeighborsParam(neighborMatrix.id, nodeIds, data, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def addNodeNeiWithNoWeightWithType(data: RDD[(Long, Long)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNodeNeiWithNoWeightWithType(pairs.toArray, isAttempt) }
    }.reduce(_ + _)
  }

  def addNodeNeiWithNoWeightWithType(pairs: Array[(Long, Long)], isAttempt: Boolean): Long = {
    val nodes2pull = new mutable.HashSet[Long]()
    nodes2pull ++= pairs.map(r => r._2)
    val types = readMsgs(nodes2pull.toArray)

    val new_pairs = pairs.groupBy(r => r._1)

    val nodeIds = new Array[Long](new_pairs.size)
    val data = new Array[IElement](new_pairs.size)

    var idx = 0
    new_pairs.foreach { case (src, nbrs) =>
      val indptr = new IntArrayList()
      val typeList = new IntArrayList()
      val (nbrs_, tags) = nbrs.map(x => (x._2, types.get(x._2))).sortWith(_._2 < _._2).unzip
      createTypeNeighbors(indptr, typeList, tags, nbrs_)
      nodeIds(idx) = src
      data(idx) = new LongIntIntArrayPairElement(nbrs_, typeList.toIntArray(), indptr.toIntArray())
      idx += 1
    }

    val func = new AddNeighborsWithType(new AddNeighborsParam(neighborMatrix.id, nodeIds, data, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def createTypeNeighbors(indptr: IntArrayList, typeList: IntArrayList, tags: Array[Int], nbrs: Array[Long]) = {
    indptr.add(0)
    var tag = tags(0)
    typeList.add(tag)
    var i = 1
    while (i < nbrs.length) {
      if (tag != tags(i)) {
        typeList.add(tags(i))
        indptr.add(i)
        tag = tags(i)
      }
      i += 1
    }
    indptr.add(nbrs.length)
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
    val func = new AddNeighbors(new AddNeighborsParam(neighborMatrix.id, nodeIds, neighbors, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def addNodeNeiWithEdgeType(data: RDD[(Long, Long, Int, Float)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNodeNeiWithEdgeType(pairs.toArray, isAttempt) }
    }.reduce(_ + _)
  }

  def addNodeNeiWithEdgeType(pairs: Array[(Long, Long, Int, Float)], isAttempt: Boolean): Long = {
    val new_pairs = pairs.groupBy(r => r._1)

    val nodeIds = new Array[Long](new_pairs.size)
    val data = new Array[IElement](new_pairs.size)

    var idx = 0
    new_pairs.foreach { case (src, nbrs) =>
      val indptr = new IntArrayList()
      val typeList = new IntArrayList()
      val (nbrs_, tags, weights) = nbrs.map(x => (x._2, x._3, x._4)).sortWith(_._2 < _._2).unzip3
      createTypeNeighbors(indptr, typeList, tags, nbrs_)
      nodeIds(idx) = src
      data(idx) = new LongIntIntFloatArrayPairElement(nbrs_, typeList.toIntArray(), indptr.toIntArray(), weights)
      idx += 1
    }
    val func = new AddWeightedNeighborsWithType(new AddNeighborsParam(neighborMatrix.id, nodeIds, data, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def addNodeNeiWithType(data: RDD[(Long, Long, Float)], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      val isAttempt = if (TaskContext.get().attemptNumber() > 0) true else false
      iter.sliding(batchSize, batchSize).map { pairs => addNodeNeiWithType(pairs.toArray, isAttempt) }
    }.reduce(_ + _)
  }

  def addNodeNeiWithType(pairs: Array[(Long, Long, Float)], isAttempt: Boolean): Long = {
    val nodes2pull = new mutable.HashSet[Long]()
    nodes2pull ++= pairs.map(r => r._2)
    val types = readMsgs(nodes2pull.toArray)

    val new_pairs = pairs.groupBy(r => r._1)

    val nodeIds = new Array[Long](new_pairs.size)
    val data = new Array[IElement](new_pairs.size)

    var idx = 0
    new_pairs.foreach { case (src, nbrs) =>
      val indptr = new IntArrayList()
      val typeList = new IntArrayList()
      val (nbrs_, tags, weights) = nbrs.map(x => (x._2, types.get(x._2), x._3)).sortWith(_._2 < _._2).unzip3
      createTypeNeighbors(indptr, typeList, tags, nbrs_)
      nodeIds(idx) = src
      data(idx) = new LongIntIntFloatArrayPairElement(nbrs_, typeList.toIntArray(), indptr.toIntArray(), weights)
      idx += 1
    }

    val func = new AddWeightedNeighborsWithType(new AddNeighborsParam(neighborMatrix.id, nodeIds, data, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
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
    val func = new AddWeightedNeighbors(new AddNeighborsParam(neighborMatrix.id, nodeIds, neighbors, isAttempt))
    neighborMatrix.psfUpdate(func).get(1800000, TimeUnit.MILLISECONDS)
    pairs.length.toLong
  }

  def createAlias(data: RDD[Long], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => createAlias(pairs.toArray) }
    }.reduce(_ + _)
  }

  def createAlias(nodeIds: Array[Long]): Long = {
    // pull weights first
    val pulledWeights = neighborMatrix.psfGet(new GetWeights(new LongKeysGetParam(neighborMatrix.id, nodeIds)))
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
    neighborMatrix.psfUpdate(new InitAlias(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, alias)))
      .get(1800000, TimeUnit.MILLISECONDS)

    nodeIds.length.toLong
  }

  def createAliasWithType(data: RDD[Long], batchSize: Int): Long = {
    data.mapPartitions { iter =>
      iter.sliding(batchSize, batchSize).map { pairs => createAliasWithType(pairs.toArray) }
    }.reduce(_ + _)
  }

  def createAliasWithType(nodeIds: Array[Long]): Long = {
    // pull weights first
    val pulledWeights = neighborMatrix.psfGet(new GetWeights(new LongKeysGetParam(neighborMatrix.id, nodeIds)))
      .asInstanceOf[GetFloatsResult].getData
    val pulledTypes = neighborMatrix.psfGet(new GetTypes(new LongKeysGetParam(neighborMatrix.id, nodeIds)))
      .asInstanceOf[GetIntsResult].getData
    val pulledIndptr = neighborMatrix.psfGet(new GetIndptr(new LongKeysGetParam(neighborMatrix.id, nodeIds)))
      .asInstanceOf[GetIntsResult].getData

    val alias = nodeIds.map { node =>
      val weightsAll = pulledWeights.get(node)
      val types = pulledTypes.get(node)
      val indptr = pulledIndptr.get(node)

      val accept = new Array[Float](weightsAll.length)
      val alias = new Array[Int](weightsAll.length)
      var p = 0

      for(i <- types.indices) {
        val min = indptr(i)
        val max = indptr(i + 1)
        val weights = weightsAll.slice(min, max)

        val weightsSum = weights.sum
        val len = weights.length
        val areaRatio = weights.map(_ / weightsSum * len)
        val (accept_, alias_) = createAliasTable(areaRatio)
        System.arraycopy(accept_, 0, accept, p, accept_.length)
        System.arraycopy(alias_, 0, alias, p, alias_.length)
        p += len
      }
      new IntFloatArrayPairElement(alias, accept).asInstanceOf[IElement]
    }

    // push to ps
    neighborMatrix.psfUpdate(new InitAlias(new LongKeysUpdateParam(neighborMatrix.id, nodeIds, alias)))
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

  def checkpoint(): Unit = {
    neighborMatrix.checkpoint()
  }

  def clearNeighborTable(): Unit = {
    // todo
  }

}