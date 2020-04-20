package com.tencent.angel.spark.ml.graph.feature.functionalEdgeAggregate

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

import it.unimi.dsi.fastutil.floats.FloatArrayList

class FunctionalEdgeAggregateGraphPartition(index: Int,
                           allNodes: Array[Long],
                           indptrAll: Array[Int],
													 neighbors: Array[Long],
                           edgeWeights: Array[Float],
                           edgeAggInfo: Array[(Float, Float, Float, Float)]) extends Serializable {
  
  def process(): (Array[Long], Array[(Float, Float, Float, Float)]) = {
    for (idx <- allNodes.indices) {
      var j = indptrAll(idx)
      val penalty_ = sqrtDegreePenalty(idx, allNodes.indexOf(neighbors(j)))
      var sumW = 0.0f
      var maxW = edgeWeights(j) * penalty_
      var minW = edgeWeights(j) * penalty_
      while (j < indptrAll(idx + 1)) {
				val nbrIdx = allNodes.indexOf(neighbors(j))
				val penalty = sqrtDegreePenalty(idx, nbrIdx)
        // calculate sum
				val penalWeight = (edgeWeights(j) * penalty)
        sumW += penalWeight
        // calculate max
        if (maxW < penalWeight) {
          maxW = penalWeight
        }
        // calculate min
        if (minW > penalWeight) {
          minW = penalWeight
        }

        j += 1
      }

      val meanW = sumW / (indptrAll(idx + 1) - indptrAll(idx))

      val aggInfo = (maxW, minW, sumW, meanW)
      edgeAggInfo(idx) = aggInfo
    }

    (allNodes, edgeAggInfo)
  }

  def sqrtDegreePenalty(nodeIdx: Int, nbrIdx: Int): Float = {
      val srcDegree = indptrAll(nodeIdx + 1) - indptrAll(nodeIdx)
      val dstDegree = indptrAll(nbrIdx + 1) - indptrAll(nbrIdx)
      val penalty = 1.0f / math.sqrt(srcDegree * dstDegree)
      penalty.toFloat
  }

  def calcFuncMax(idx: Int): Float = {
    var j = indptrAll(idx)
    val penalty_ = sqrtDegreePenalty(idx, allNodes.indexOf(neighbors(j)))
    var maxW = edgeWeights(j) * penalty_
    while (j < indptrAll(idx + 1)) {
      val penalty = sqrtDegreePenalty(idx, allNodes.indexOf(neighbors(j)))
			val penalWeight = edgeWeights(j) * penalty
      if (maxW < penalWeight) {
        maxW = penalWeight
      }
      j += 1
    }
    maxW
  }

  def calcFuncMin(idx: Int, penalty: Float): Float = {
    var j = indptrAll(idx)
    val penalty_ = sqrtDegreePenalty(idx, allNodes.indexOf(neighbors(j)))
    var minW = edgeWeights(j) * penalty_
    while (j < indptrAll(idx + 1)) {
      val penalty = sqrtDegreePenalty(idx, allNodes.indexOf(neighbors(j)))
      val penalWeight = edgeWeights(j) * penalty
      if (minW > penalWeight) {
        minW = penalWeight
      }
      j += 1
    }
    minW
  }

  def calcFuncSum(idx: Int, penalty: Float): Float = {
    var j = indptrAll(idx)
    var sumW = 0.0f
    while (j < indptrAll(idx + 1)) {
      val penalty = sqrtDegreePenalty(idx, allNodes.indexOf(neighbors(j)))
      sumW += (edgeWeights(j) * penalty)
      j += 1
    }
    sumW
  }

  def calcFuncMean(idx: Int, penalty: Float): Float = {
    var sumW = calcFuncSum(idx, penalty)
    val meanW = sumW / (indptrAll(idx + 1) - indptrAll(idx))
    meanW
  }

  def save(): (Array[Long], Array[(Float, Float, Float, Float)]) =
    (allNodes, edgeAggInfo)

}

object FunctionalEdgeAggregateGraphPartition {


  def apply(index: Int, iteratorAll: Iterator[(Long, Iterable[(Long, Float)])]): FunctionalEdgeAggregateGraphPartition = {
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
		val neighborsArray = allNeighbors.toLongArray()
    val edgeWeightArray = edgeWeights.toFloatArray()

    new FunctionalEdgeAggregateGraphPartition(index,
      allNodesArray,
      csrAllPointer.toIntArray(),
			neighborsArray,
      edgeWeightArray,
      new Array[(Float, Float, Float, Float)](allNodesArray.length)
    )
  }
}
