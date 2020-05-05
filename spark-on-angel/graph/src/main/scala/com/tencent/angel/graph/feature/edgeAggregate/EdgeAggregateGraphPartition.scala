package com.tencent.angel.graph.feature.edgeAggregate

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList

class EdgeAggregateGraphPartition(index: Int,
                           allNodes: Array[Long],
                           indptrAll: Array[Int],
                           edgeWeights: Array[Float],
                           edgeAggInfo: Array[(Float, Float, Float, Float)]) extends Serializable {
  
  def process(): (Array[Long], Array[(Float, Float, Float, Float)]) = {
    for (idx <- allNodes.indices) {
      var j = indptrAll(idx)
      var sumW = 0.0f
      var maxW = edgeWeights(j)
      var minW = edgeWeights(j)
      while (j < indptrAll(idx + 1)) {
        // calculate sum
        sumW += edgeWeights(j)
        // calculate max
        if (maxW < edgeWeights(j)) {
          maxW = edgeWeights(j)
        }
        // calculate min
        if (minW > edgeWeights(j)) {
          minW = edgeWeights(j)
        }

        j += 1
      }

      val meanW = sumW / (indptrAll(idx + 1) - indptrAll(idx))

      val aggInfo = (maxW, minW, sumW, meanW)
      edgeAggInfo(idx) = aggInfo
    }

    (allNodes, edgeAggInfo)
  }

  def calcMax(idx: Int): Float = {
    var j = indptrAll(idx)
    var maxW = edgeWeights(j)
    while (j < indptrAll(idx + 1)) {
      if (maxW < edgeWeights(j)) {
        maxW = edgeWeights(j)
      }
      j += 1
    }
    maxW
  }

  def calcMin(idx: Int): Float = {
    var j = indptrAll(idx)
    var minW = edgeWeights(j)
    while (j < indptrAll(idx + 1)) {
      if (minW > edgeWeights(j)) {
        minW = edgeWeights(j)
      }
      j += 1
    }
    minW
  }

  def calcSum(idx: Int): Float = {
    var j = indptrAll(idx)
    var sumW = 0.0f
    while (j < indptrAll(idx + 1)) {
      sumW += edgeWeights(j)
      j += 1
    }
    sumW
  }

  def calcMean(idx: Int): Float = {
    var sumW = calcSum(idx)
    val meanW = sumW / (indptrAll(idx + 1) - indptrAll(idx))
    meanW
  }

  def save(): (Array[Long], Array[(Float, Float, Float, Float)]) =
    (allNodes, edgeAggInfo)

}

object EdgeAggregateGraphPartition {


  def apply(index: Int, iteratorAll: Iterator[(Long, Iterable[(Long, Float)])]): EdgeAggregateGraphPartition = {
    val csrAllPointer = new IntArrayList()
    csrAllPointer.add(0)

    val allNodes = new LongArrayList()

    val allNeighbors = new LongArrayList()

    val edgeWeights = new FloatArrayList()

    var maxDegree: Int = 0
    while (iteratorAll.hasNext) {
        val (node, ns) = iteratorAll.next()
        allNodes.add(node)

        ns.toArray.distinct.foreach(n => {
          allNeighbors.add(n._1)
          edgeWeights.add(n._2)})
        
        csrAllPointer.add(allNeighbors.size())

        maxDegree = math.max(ns.size, maxDegree)
        
    }

    val allNodesArray = allNodes.toLongArray()
    val edgeWeightArray = edgeWeights.toFloatArray()

    new EdgeAggregateGraphPartition(index,
      allNodesArray,
      csrAllPointer.toIntArray(),
      edgeWeightArray,
      new Array[(Float, Float, Float, Float)](allNodesArray.length)
    )
  }
}
