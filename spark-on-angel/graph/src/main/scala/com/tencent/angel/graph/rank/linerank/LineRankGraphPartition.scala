package com.tencent.angel.graph.rank.linerank

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2FloatOpenHashMap, LongArrayList, LongOpenHashSet}

private[linerank]
class LineRankGraphPartition(index: Int,
                             keys: Array[Long],
                             indptr: Array[Int],
                             inNodes: Array[Long],
                             weights: Array[Float],
                             outDegrees: Array[Float],
                             lineranks: Array[Float],
                             numActives: Int) extends Serializable {

  assert(keys.length == indptr.length - 1)

  def initOutDegs(model: LineRankPSModel): LineRankGraphPartition = {
    val srcOutDegs = model.readValues(keys.clone())
    for (idx <- keys.indices) {
      val srcOutDeg = srcOutDegs.get(keys(idx))
      var j = indptr(idx)
      while (j < indptr(idx + 1)) {
        outDegrees(j) = weights(j) * srcOutDeg
        j += 1
      }
    }
    new LineRankGraphPartition(index, keys, indptr, inNodes,
      weights, outDegrees, lineranks, numActives)
  }

  def aggTargetIndices(model: LineRankPSModel): Unit = {
    val update = VFactory.sparseLongKeyFloatVector(model.dim)
    for (idx <- keys.indices) {
      var dstRank = 0.0f
      var j = indptr(idx)
      while (j < indptr(idx + 1)) {
        if (outDegrees(j) > 0)
          dstRank += (weights(j) * lineranks(j)) / outDegrees(j)
        j += 1
      }
      update.set(keys(idx), dstRank)
    }
    model.sendMsgs(update)
  }

  def batchedAggSrcIncidence(model: LineRankPSModel, resetProb: Float, tol: Float, numBatch: Int): LineRankGraphPartition = {
    if (numActives == 0)
      return new LineRankGraphPartition(index, keys, indptr, inNodes,
        weights, outDegrees, lineranks, 0)
    val batchSize = inNodes.length / numBatch
    var start = 0
    var end = math.min(inNodes.length, start + batchSize)
    var numActive = 0
    while (start < end && end <= inNodes.length) {
      val activeIns = activeInNodes(start, end)
      if (activeIns.length > 0) {
        val srcRanks = model.readValues(activeIns)
        for (idx <- start until end) if (outDegrees(idx) > 0) {
          val rank = srcRanks.get(inNodes(idx)) * weights(idx) * (1 - resetProb)
          if (Math.abs(rank - lineranks(idx)) < tol) {
            outDegrees(idx) = 0
          } else {
            lineranks(idx) = rank
            numActive += 1
          }
        }
      }
      start = end
      end = math.min(outDegrees.length, start + batchSize)
    }
    new LineRankGraphPartition(index, keys, indptr, inNodes,
      weights, outDegrees, lineranks, numActive)
  }

  def getNumActives: Long = numActives

  def calcNodeCentrality(model: LineRankPSModel, numBatch: Int): Unit = {
    val batchSize = math.max(keys.length / numBatch, 100)
    val outMsgs = new Long2FloatOpenHashMap()
    var idx = 0
    while (idx < keys.length) {
      var j = indptr(idx)
      while (j < indptr(idx + 1)) {
        outMsgs.addTo(keys(idx), weights(j) * lineranks(j))
        outMsgs.addTo(inNodes(j), weights(j) * lineranks(j))
        j += 1
      }
      if (idx > 0 && idx % batchSize == 0 || idx == keys.length - 1) {
        val update = VFactory.sparseLongKeyFloatVector(model.dim)
        update.setStorage(new LongFloatSparseVectorStorage(update.dim(), outMsgs))
        model.sendMsgs(update)
        outMsgs.clear()
      }
      idx += 1
    }
  }

  def activeInNodes(startIdx: Int, endIdx: Int): Array[Long] = {
    val activeNodes = new LongOpenHashSet()
    for (idx <- startIdx until endIdx) {
      if (outDegrees(idx) > 0)
        activeNodes.add(inNodes(idx))
    }
    activeNodes.toLongArray()
  }

  def save(model: LineRankPSModel): (Array[Long], Array[Float]) = {
    val retMap = model.readValues(keys.clone())
    (keys, keys.map { key => retMap.get(key)} )
  }
}

private[linerank]
object LineRankGraphPartition {
  def apply(model: LineRankPSModel,
            index: Int,
            iter: Iterator[(Long, Iterable[(Long, Float)])],
            resetProb: Float,
            msgNumBatch: Int): LineRankGraphPartition = {
    val indptr = new IntArrayList()
    val inNodes = new LongArrayList()
    val keys = new LongArrayList()
    val outMsgs = new Long2FloatOpenHashMap()
    val weights = new FloatArrayList()

    indptr.add(0)
    var idx = 0
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, srcs) = (entry._1, entry._2)
      srcs.foreach { case (n, weight) =>
        inNodes.add(n)
        outMsgs.addTo(n, weight)
        weights.add(weight)
      }
      indptr.add(inNodes.size())
      keys.add(node)
      idx += 1
    }

    val it = outMsgs.long2FloatEntrySet().fastIterator()
    val batchSize = outMsgs.size() / msgNumBatch
    var i = 0
    val batchOutMsg = new Long2FloatOpenHashMap()
    while (it.hasNext) {
      i += 1
      val entry = it.next()
      batchOutMsg.addTo(entry.getLongKey, entry.getFloatValue)
      if (i % batchSize == 0 || (!it.hasNext)) {
        val update = VFactory.sparseLongKeyFloatVector(model.dim)
        update.setStorage(new LongFloatSparseVectorStorage(update.dim(), batchOutMsg))
        model.sendMsgs(update)
        batchOutMsg.clear()
      }
    }

    new LineRankGraphPartition(index,
      keys.toLongArray(),
      indptr.toIntArray(),
      inNodes.toLongArray(),
      weights.toFloatArray(),
      new Array[Float](inNodes.size()),
      Array.fill(inNodes.size())(resetProb),
      1)
  }
}
