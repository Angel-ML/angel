package com.tencent.angel.spark.ml.graph.algo.embedding.metaPath2vec

import it.unimi.dsi.fastutil.ints.IntArrayList
import com.tencent.angel.graph.model.neighbor.simplewithtype.TypeNeighborElement
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.ps.storage.vector.element.IElement
import it.unimi.dsi.fastutil.longs.LongArrayList
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MetaPath2VecOperator {

  def initNeighborTableWithType(iter: Iterator[(Long, Array[(Long, Float)])], batchSize: Int,
                                model: MetaPath2VecPSModel, isWeighted: Boolean = false) = {
    BatchIter(iter, batchSize).foreach { batchIter =>
      val start = System.currentTimeMillis()
      val nodes2pull = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, nbrs) =>
        nodes2pull ++= nbrs.map(_._1)
        nodes2pull.add(src)
      }
      val beforePull = System.currentTimeMillis()
      val types = model.readMsgs(nodes2pull.toArray)
      println(s"pull ${nodes2pull.size} nodes' type, cost ${System.currentTimeMillis() - beforePull} ms.")

      val nodeIds = new Array[Long](batchIter.length)
      val data = new Array[IElement](batchIter.length)
      for (idx <- batchIter.indices) {
        val (key, nbrs) = batchIter(idx)
        val indptr = new IntArrayList()
        val typeList = new IntArrayList()
        if (isWeighted) {
          val (nbrs_, tags, weights) = nbrs.map(x => (x._1, types.get(x._1), x._2)).sortWith(_._2 < _._2).unzip3
          val accept = new Array[Float](nbrs_.length)
          val alias = new Array[Int](nbrs_.length)
          createTypeNeighbors(indptr, typeList, accept, alias, tags, nbrs_, weights)
          nodeIds(idx) = key
          data(idx) = new TypeNeighborElement(types.get(key), nbrs_, accept, alias, typeList.toIntArray(), indptr.toIntArray())
        } else {
          val (nbrs_, tags) = nbrs.map(x => (x._1, types.get(x._1))).sortWith(_._2 < _._2).unzip
          createTypeNeighbors(indptr, typeList, tags, nbrs_)
          nodeIds(idx) = key
          data(idx) = new TypeNeighborElement(types.get(key), nbrs_, typeList.toIntArray(), indptr.toIntArray())
        }
      }
      val beforePush = System.currentTimeMillis()
      model.initNeighbors(nodeIds, data)
      println(s"initNeighborTableWithType: last batch cost ${System.currentTimeMillis()-start} ms," +
        s"push ${batchIter.length} neighborTables to ps, cost ${System.currentTimeMillis()-beforePush} ms.")
    }
  }

  def initNeighborTableWithEdgeType(iter: Iterator[(Long, Array[(Long, Int, Float)])], batchSize: Int,
                                model: MetaPath2VecPSModel, isWeighted: Boolean = false) = {
    BatchIter(iter, batchSize).foreach { batchIter =>
      val start = System.currentTimeMillis()
      val nodeIds = new Array[Long](batchIter.length)
      val data = new Array[IElement](batchIter.length)
      for (idx <- batchIter.indices) {
        val (key, nbrs) = batchIter(idx)
        val indptr = new IntArrayList()
        val typeList = new IntArrayList()
        if (isWeighted) {
          val (nbrs_, tags, weights) = nbrs.sortWith(_._2 < _._2).unzip3
          val accept = new Array[Float](nbrs_.length)
          val alias = new Array[Int](nbrs_.length)
          createTypeNeighbors(indptr, typeList, accept, alias, tags, nbrs_, weights)
          nodeIds(idx) = key
          // set selfType to be -1 since src node has no type
          data(idx) = new TypeNeighborElement(-1, nbrs_, accept, alias, typeList.toIntArray(), indptr.toIntArray())
        } else {
          val (nbrs_, tags) = nbrs.map(x => (x._1, x._2)).sortWith(_._2 < _._2).unzip
          createTypeNeighbors(indptr, typeList, tags, nbrs_)
          nodeIds(idx) = key
          data(idx) = new TypeNeighborElement(-1, nbrs_, typeList.toIntArray(), indptr.toIntArray())
        }
      }
      val beforePush = System.currentTimeMillis()
      model.initNeighbors(nodeIds, data)
      println(s"initNeighborTableWithType: last batch cost ${System.currentTimeMillis()-start} ms," +
        s"push ${batchIter.length} neighborTables to ps, cost ${System.currentTimeMillis()-beforePush} ms.")
    }
  }

  def createTypeNeighbors(indptr: IntArrayList, typeList: IntArrayList, tags: Array[Int], nbrs: Array[Long]) = {
    indptr.add(0)
    var tag =tags(0)
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

  def createTypeNeighbors(indptr: IntArrayList, typeList: IntArrayList, accept: Array[Float], alias: Array[Int],
                          tags: Array[Int], nbrs: Array[Long], weights: Array[Float]) = {
    indptr.add(0)
    var tag =tags(0)
    var p = 0
    typeList.add(tag)
    var i = 1
    while (i < nbrs.length) {
      if (tag != tags(i)) {
        typeList.add(tags(i))
        indptr.add(i)
        tag = tags(i)
        val (accept_, alias_) = createAliasTable(weights.slice(p, i))
        System.arraycopy(accept_, 0, accept, p, accept_.length)
        System.arraycopy(alias_, 0, alias, p, alias_.length)
        p = i
      }
      i += 1
    }
    val (accept_, alias_) = createAliasTable(weights.slice(p, i))
    System.arraycopy(accept_, 0, accept, p, accept_.length)
    System.arraycopy(alias_, 0, alias, p, alias_.length)
    indptr.add(nbrs.length)
  }

  def initNeighborTable(iter: Iterator[(Long, Array[(Long, Float)])], batchSize: Int,
                        model: MetaPath2VecPSModel, isWeighted: Boolean = false) = {
    BatchIter(iter, batchSize).foreach { batchIter =>
      val start = System.currentTimeMillis()
      val nodeIds = new Array[Long](batchIter.length)
      val data = new Array[IElement](batchIter.length)
      for (idx <- batchIter.indices) {
        nodeIds(idx) = batchIter(idx)._1
        val (nbrs, weights) = batchIter(idx)._2.unzip
        if (isWeighted) {
          val (accept, alias) = createAliasTable(weights)
          data(idx) = new TypeNeighborElement(nbrs, accept, alias)
        }
        else {
          data(idx) = new TypeNeighborElement(nbrs)
        }
      }
      val beforePush = System.currentTimeMillis()
      model.initNeighbors(nodeIds, data)
      println(s"initNeighborTable: last batch cost ${System.currentTimeMillis()-start} ms," +
        s"push ${batchIter.length} neighborTables to ps, cost ${System.currentTimeMillis()-beforePush} ms.")
    }
  }

  def initNeighborTableWithEdgeType_(rawEdges: RDD[(Long, Long, Int, Float)], degree: RDD[(Long, Array[(Int, Int)])], batchSize: Int,
                                 model: MetaPath2VecPSModel, isWeighted: Boolean = false) = {
    var startTime = System.currentTimeMillis()
    val initNumNodes = model.initNeighborsDegreeWithType(degree, batchSize, isWeighted)
    println(s"init $initNumNodes nodes' space to PS, cost ${System.currentTimeMillis() - startTime} ms.")

    if (isWeighted) {
      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNeiWithEdgeType(rawEdges, batchSize)
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")

      // pull weights and calculate alias and push to ps
      startTime = System.currentTimeMillis()
      val transNumNodes = model.createAliasWithType(degree.map(_._1), batchSize)
      println(s"created $transNumNodes nodes' alias, cost ${System.currentTimeMillis() - startTime} ms.")
    } else {
      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNeiWithNoWeightWithEdgeType(rawEdges.map(r => (r._1, r._2, r._3)), batchSize)
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")
    }
  }

  def initNeighborTableWithType_(rawEdges: RDD[(Long, Long, Float)], degree: RDD[(Long, Array[(Int, Int)])], batchSize: Int,
                                model: MetaPath2VecPSModel, isWeighted: Boolean = false) = {
    var startTime = System.currentTimeMillis()
    val initNumNodes = model.initNeighborsDegreeWithType(degree, batchSize, isWeighted)
    println(s"init $initNumNodes nodes' space to PS, cost ${System.currentTimeMillis() - startTime} ms.")

    if (isWeighted) {
      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNeiWithType(rawEdges, batchSize)
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")

      // pull weights and calculate alias and push to ps
      startTime = System.currentTimeMillis()
      val transNumNodes = model.createAliasWithType(degree.map(_._1), batchSize)
      println(s"created $transNumNodes nodes' alias, cost ${System.currentTimeMillis() - startTime} ms.")
    } else {
      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNeiWithNoWeightWithType(rawEdges.map(r => (r._1, r._2)), batchSize)
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")
    }
  }

  def initNeighborTable_(rawEdges: RDD[(Long, Long, Float)], degree: RDD[(Long, Int)], batchSize: Int,
                        model: MetaPath2VecPSModel, isWeighted: Boolean = false) = {
    var startTime = System.currentTimeMillis()
    val initNumNodes = model.initNeighborsDegree(degree, batchSize, isWeighted)
    println(s"init $initNumNodes nodes' space to PS, cost ${System.currentTimeMillis() - startTime} ms.")

    if (isWeighted) {
      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNei(rawEdges, batchSize)
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")

      // pull weights and calculate alias and push to ps
      startTime = System.currentTimeMillis()
      val transNumNodes = model.createAlias(degree.map(_._1), batchSize)
      println(s"created $transNumNodes nodes' alias, cost ${System.currentTimeMillis() - startTime} ms.")
    } else {
      startTime = System.currentTimeMillis()
      val numEdges = model.addNodeNeiWithNoWeight(rawEdges.map(r => (r._1, r._2)), batchSize)
      println(s"add $numEdges edges to PS, cost ${System.currentTimeMillis() - startTime} ms.")
    }
  }

  // if sampleByByType is false, process random sampling even when there are typed neighbors;
  // eg. for GATNE walking, when the overall nodeTypePath is provided while no metaPath exists for
  // a certain edgeType, set sampleByType to false
  def sample(index: Int, iter: Iterator[(Long, Int)], thisPath: Array[Int],
             model: MetaPath2VecPSModel, batchSize: Int=100000, sampleByType: Boolean=true, dynamicInitNeighbor: Boolean=false) = {
    BatchIter(iter, batchSize).flatMap { batchIter =>
      val walkLen = thisPath.length + 1
      val result = batchIter.map(x => Array(x._1) ++ new Array[Long](thisPath.length))
      val endIdx = Array.fill(batchIter.length)(walkLen)
      var step = 0
      val sampleNodes = new LongArrayList()
      batchIter.foreach(x => sampleNodes.add(x._1))
      while (step < thisPath.length && (!sampleNodes.isEmpty)) {
        val sampleType = thisPath(step)
        val startTime = System.currentTimeMillis()
        val sampled = model.sample(sampleNodes.toLongArray, sampleType, sampleByType, dynamicInitNeighbor) // make sure the returned array is aligned with the sampleNodes
        val pullTime = System.currentTimeMillis() - startTime
        sampleNodes.clear()
        for (idx <- result.indices) {
          if (endIdx(idx) == walkLen) {
            val key = result(idx)(step)
            val next = sampled.get(key).next()
            if (next != key) {
              result(idx)(step+1) = next
              sampleNodes.add(next)
            } else {
              endIdx(idx) = step+1
            }
          }
        }
        println(s"step $step sampling: pulling and processing cost ${System.currentTimeMillis()-startTime} ms, " +
          s"pulling cost $pullTime ms.")
        step += 1
      }
      result.zip(endIdx).filter(_._2 > 1).map(x => Row(x._1.slice(0, x._2).mkString(" ")))
//      result.filter(_.length > 1).map(x => Row(x.mkString(" ")))
    }
  }

  def createAliasTable(weights: Array[Float]): (Array[Float], Array[Int]) = {
    val weightsSum = weights.sum
    val len = weights.length
    val areaRatio = weights.map(_ / weightsSum * len)

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
}