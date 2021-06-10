package com.tencent.angel.graph.community.hanp

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

import scala.collection.mutable
import scala.util.Random

class HANPGraphPartition(index: Int,
                         preserveRate: Float,
                         delta: Float,
                         keys: Array[Long],
                         indptr: Array[Int],
                         neighbors: Array[Long],
                         keyLabels: Array[(Long, Float)],
                         indices: Array[Long],
                         weights: Array[Float]) extends Serializable {

  def initDegrees(model: HANPPSModel): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), indptr(i + 1) - indptr(i))
    model.initDegree(msgs)
    msgs.size().toInt
  }

  def initScores(model: HANPPSModel): Int = {
    val msgs = VFactory.sparseLongKeyFloatVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), 1.0f)
    model.initScore(msgs)
    msgs.size().toInt
  }

  def initLabels(model: HANPPSModel): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(model.dim)
    for (i <- keys.indices)
      msgs.set(keys(i), keys(i))
    model.initLabel(msgs)
    msgs.size().toInt
  }

  def process(model: HANPPSModel): HANPGraphPartition = {
    // pull msgs
    val beforePullTs = System.currentTimeMillis()
    val scoreInMsgs = model.readScore(indices)
    val degrees = model.readDegree(indices)
    val labels = model.readLabel(indices)
    println(s"partition $index: process, " +
      s"pull ${indices.length} nodes from ps, " +
      s"cost ${System.currentTimeMillis() - beforePullTs} ms")

    // calculate
    val beforeComputeTs = System.currentTimeMillis()
    val labelOutMsgs = VFactory.sparseLongKeyLongVector(keys.length)
    val scoreOutMsgs = VFactory.sparseLongKeyFloatVector(keys.length)
    for (idx <- keys.indices) {
      val (newLabel, newScore) = if (Random.nextDouble() < preserveRate) {
        keyLabels(idx)
      } else {
        calc(idx, scoreInMsgs, degrees, labels)
      }
      labelOutMsgs.set(keys(idx), newLabel)
      scoreOutMsgs.set(keys(idx), newScore)
      keyLabels(idx) = (newLabel, newScore)
    }
    println(s"partition $index: process, " +
      s"compute ${keys.length} nodes, " +
      s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")

    val beforePushTs = System.currentTimeMillis()
    model.writeLabel(labelOutMsgs)
    model.writeScore(scoreOutMsgs)
    println(s"partition $index: process, " +
      s"push ${keys.length} nodes to ps, " +
      s"cost ${System.currentTimeMillis() - beforePushTs} ms")


    new HANPGraphPartition(index, preserveRate, delta, keys, indptr,
      neighbors, keyLabels, indices, weights)
  }

  def calc(idx: Int, scoreInMsgs: LongFloatVector, degrees: LongLongVector,
           labelInMsgs: LongLongVector): (Long, Float) = {
    var j = indptr(idx)
    val temp = mutable.Map(-1l -> (0f, 0f))
    while (j < indptr(idx + 1)) {
      val l = labelInMsgs.get(neighbors(j))
      val s = math.max(scoreInMsgs.get(neighbors(j)), 0.0f)
      val d = degrees.get(neighbors(j))
      val w = weights(j)
      val labelToChoose = s * d * w
      val (inLabelToChoose, inScore) = temp.getOrElse(l, (0.0f, 0.0f))
      temp += ((l, (labelToChoose + inLabelToChoose, math.max(inScore, s))))
      j += 1
    }
    temp.remove(-1)
    assert(temp.nonEmpty, s"mutable map for key ${keys(idx)} is empty.")
    val newLabel = temp.maxBy(_._2._1)
    if (newLabel._1 == keyLabels(idx)._1) {
      (newLabel._1, newLabel._2._1)
    } else {
      (newLabel._1, newLabel._2._1 - delta)
    }
  }

  def save(): (Array[Long], Array[Long]) =
    (keys, keyLabels.map(_._1))
}


object HANPGraphPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])], delta: Float,
            preserveRate: Float): HANPGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()
    val weights = new FloatArrayList()

    indptr.add(0)
    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2)
      ns.toArray.distinct.sorted.foreach { case (n, w) =>
        neighbours.add(n)
        weights.add(w)
      }
      indptr.add(neighbours.size())
      keys.add(node)
      maxDegree = math.max(ns.size, maxDegree)
    }

    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()
    val weightsArray = weights.toFloatArray()

    new HANPGraphPartition(index,
      preserveRate,
      delta,
      keysArray,
      indptr.toIntArray(),
      neighboursArray,
      keysArray.map(x=>(x, 1f)), // initial key label and score
      keysArray.union(neighboursArray).distinct,
      weightsArray)
  }

}
