package com.tencent.angel.spark.ml.graph.algo.feature.functionalEdgeAggregate

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

import it.unimi.dsi.fastutil.floats.FloatArrayList

/**
 * Neighbor degrees are provided per edge (globally computed), so the aggregation is
 * correct no matter how nodes are distributed across partitions.
 */
class FunctionalEdgeAggregateGraphPartition(index: Int,
                           allNodes: Array[Long],
                           indptrAll: Array[Int],
                           neighbors: Array[Long],
                           edgeWeights: Array[Float],
                           neighborDegrees: Array[Int],
                           edgeAggInfo: Array[(Float, Float, Float, Float)]) extends Serializable {

  def process(): (Array[Long], Array[(Float, Float, Float, Float)]) = {
    for (idx <- allNodes.indices) {
      var j = indptrAll(idx)
      val penalty_ = sqrtDegreePenalty(idx, j)
      var sumW = 0.0f
      var maxW = edgeWeights(j) * penalty_
      var minW = edgeWeights(j) * penalty_
      while (j < indptrAll(idx + 1)) {
        val penalty = sqrtDegreePenalty(idx, j)
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

  def sqrtDegreePenalty(nodeIdx: Int, edgeIdx: Int): Float = {
      val srcDegree = indptrAll(nodeIdx + 1) - indptrAll(nodeIdx)
      val dstDegree = neighborDegrees(edgeIdx)
      val penalty = 1.0f / math.sqrt(srcDegree.toDouble * dstDegree)
      penalty.toFloat
  }

  def calcFuncMax(idx: Int): Float = {
    var j = indptrAll(idx)
    var maxW = edgeWeights(j) * sqrtDegreePenalty(idx, j)
    while (j < indptrAll(idx + 1)) {
      val penalWeight = edgeWeights(j) * sqrtDegreePenalty(idx, j)
      if (maxW < penalWeight) {
        maxW = penalWeight
      }
      j += 1
    }
    maxW
  }

  def calcFuncMin(idx: Int, penalty: Float): Float = {
    var j = indptrAll(idx)
    var minW = edgeWeights(j) * sqrtDegreePenalty(idx, j)
    while (j < indptrAll(idx + 1)) {
      val penalWeight = edgeWeights(j) * sqrtDegreePenalty(idx, j)
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
      sumW += (edgeWeights(j) * sqrtDegreePenalty(idx, j))
      j += 1
    }
    sumW
  }

  def calcFuncMean(idx: Int, penalty: Float): Float = {
    val sumW = calcFuncSum(idx, penalty)
    val meanW = sumW / (indptrAll(idx + 1) - indptrAll(idx))
    meanW
  }

  def save(): (Array[Long], Array[(Float, Float, Float, Float)]) =
    (allNodes, edgeAggInfo)

}

object FunctionalEdgeAggregateGraphPartition {

  /**
   * @param iteratorAll node -> distinct (neighbor, weight, neighborDegree)
   */
  def apply(index: Int,
            iteratorAll: Iterator[(Long, Iterable[(Long, Float, Int)])]): FunctionalEdgeAggregateGraphPartition = {
    val csrAllPointer = new IntArrayList()
    csrAllPointer.add(0)

    val allNodes = new LongArrayList()
    val allNeighbors = new LongArrayList()
    val edgeWeights = new FloatArrayList()
    val neighborDegrees = new IntArrayList()

    while (iteratorAll.hasNext) {
      val (node, ns) = iteratorAll.next()
      allNodes.add(node)

      ns.foreach { case (n, w, d) =>
        allNeighbors.add(n)
        edgeWeights.add(w)
        neighborDegrees.add(d)
      }
      csrAllPointer.add(allNeighbors.size())
    }

    val allNodesArray = allNodes.toLongArray()

    new FunctionalEdgeAggregateGraphPartition(index,
      allNodesArray,
      csrAllPointer.toIntArray(),
      allNeighbors.toLongArray(),
      edgeWeights.toFloatArray(),
      neighborDegrees.toIntArray(),
      new Array[(Float, Float, Float, Float)](allNodesArray.length)
    )
  }
}
