package com.tencent.angel.spark.ml.graph.feature.interactRate

import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList

class InteractGraphPartition(index: Int,
                             keys: Array[Long],
                             indptr: Array[Int],
                             srcFlags: Array[Boolean],
                             dstFlags: Array[Boolean],
                             degrees: Array[Int],
                             outDegrees: Array[Int],
                             inDegrees: Array[Int],
                             neighbors: Array[Long],
                             srcNbrFlags: Array[Boolean],
                             dstNbrFlags: Array[Boolean],
                             outWeights: Array[Float],
                             inWeights: Array[Float],
                             rateInfo: Array[(Float, Float)]) extends Serializable {
  
  def process(): (Array[Long], Array[(Float, Float)]) = {
    val (_, wInfo) = weightInfo()
    for (idx <- keys.indices) {
      val symmetryEdge = outDegrees(idx) + inDegrees(idx) - degrees(idx)
      val inDegreeRate = wInfo(idx)._1 / (wInfo(idx)._1 + wInfo(idx)._2)
      val symmetryRate = symmetryEdge.toDouble / degrees(idx)
      rateInfo(idx) = (inDegreeRate.toFloat, symmetryRate.toFloat)
    }
    (keys, rateInfo)
  }

  def weightInfo(): (Array[Long], Array[(Float, Float)]) = {
    val weightInfo = new Array[(Float, Float)](keys.length)
    for (idx <- keys.indices) {
      var inW = 0.0f
      var outW = 0.0f
      if (srcFlags(idx)) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          if (srcNbrFlags(j)) {
            outW += outWeights(j)
          }
          j += 1
        }
      }
      if (dstFlags(idx)) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          if (dstNbrFlags(j)) {
            inW += inWeights(j)
          }
          j += 1
        }
      }
      
      weightInfo(idx) = (inW, outW)
    }
    (keys, weightInfo)
  }

  def save(): (Array[Long], Array[(Float, Float)]) =
    (keys, rateInfo)

}

object InteractGraphPartition {
  
  def apply(index: Int, iteratorAll: Iterator[(Long, Iterable[(Long, (Boolean, Float))])]): InteractGraphPartition = {
    val csrAllPointer = new IntArrayList()
    csrAllPointer.add(0)

    val allNodes = new LongArrayList()
    val srcFlags = new BooleanArrayList()
    val dstFlags = new BooleanArrayList()
    val degrees = new IntArrayList()
    val outDegrees = new IntArrayList()
    val inDegrees = new IntArrayList()
    
    val neighbors = new LongArrayList()
    val srcNbrFlags = new BooleanArrayList()
    val dstNbrFlags = new BooleanArrayList()
    val outWeights = new FloatArrayList()
    val inWeights = new FloatArrayList()

    var maxDegree: Int = 0
    while (iteratorAll.hasNext) {
      val (node, ns) = iteratorAll.next()
      allNodes.add(node)
      val (nt, bwst) = ns.unzip
      val (bst, _) = bwst.unzip
      degrees.add(nt.toArray.distinct.length)
      
      val sdArray = bst.toArray.distinct
      if (sdArray.length == 2) {
        srcFlags.add(true)
        dstFlags.add(true)
      }
      else {
        if (sdArray.head) {
          srcFlags.add(true)
          dstFlags.add(false)
        }
        else {
          srcFlags.add(false)
          dstFlags.add(true)
        }
      }
      var srcDegreeCnt = 0
      var dstDegreeCnt = 0
      ns.toArray.distinct.foreach(n => {
        neighbors.add(n._1)
        if (n._2._1) {
          srcDegreeCnt += 1
          srcNbrFlags.add(true)
          dstNbrFlags.add(false)
          outWeights.add(n._2._2)
          inWeights.add(0.0f)
        }
        else {
          dstDegreeCnt += 1
          srcNbrFlags.add(false)
          dstNbrFlags.add(true)
          outWeights.add(0.0f)
          inWeights.add(n._2._2)
        }
      })
      outDegrees.add(srcDegreeCnt)
      inDegrees.add(dstDegreeCnt)
      
      csrAllPointer.add(neighbors.size())
      
    }

    val allNodesArray = allNodes.toLongArray()
    val srcFlagsArray = srcFlags.toBooleanArray()
    val dstFlagsArray = dstFlags.toBooleanArray()
    
    val degreesArray = degrees.toIntArray()
    val outDegreesArray = outDegrees.toIntArray()
    val inDegreesArray = inDegrees.toIntArray()
    
    val outWeightsArray = outWeights.toFloatArray()
    val inWeightsArray = inWeights.toFloatArray()
    
    val neighborsArray = neighbors.toLongArray()
    val srcNbrFlagsArray = srcNbrFlags.toBooleanArray()
    val dstNbrFlagsArray = dstNbrFlags.toBooleanArray()
    
    new InteractGraphPartition(index,
      allNodesArray,
      csrAllPointer.toIntArray(),
      srcFlagsArray,
      dstFlagsArray,
      degreesArray,
      outDegreesArray,
      inDegreesArray,
      neighborsArray,
      srcNbrFlagsArray,
      dstNbrFlagsArray,
      outWeightsArray,
      inWeightsArray,
      new Array[(Float, Float)](allNodesArray.length)
    )
  }
}
