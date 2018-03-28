package com.tencent.angel.ml.optimizer2.lossfuncs

import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseFloatVector, SparseDoubleVector, SparseFloatVector}

object SoftMaxLoss {
  def apply(ymodel:TVector, yture:Double):Double = {
    val pidx = yture.toInt
    ymodel match {
      case v: DenseDoubleVector =>
        val logsumexp = Math.log(v.getValues.foldLeft(0.0){ case (last, value) => last + Math.exp(value)})
        logsumexp - v.get(pidx)
      case v: DenseFloatVector =>
        val logsumexp = Math.log(v.getValues.foldLeft(0.0){ case (last, value) => last + Math.exp(value)})
        logsumexp - v.get(pidx)
      case v: SparseDoubleVector =>
        val iter = v.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
        var sumexp = 1.0 * v.size()
        while(iter.hasNext) {
          val entry = iter.next()
          sumexp += Math.exp(entry.getDoubleValue)
        }
        Math.log(sumexp) -  v.get(pidx)
      case v: SparseFloatVector =>
        val iter = v.getIndexToValueMap.int2FloatEntrySet().fastIterator()
        var sumexp = 1.0 * v.size()
        while(iter.hasNext) {
          val entry = iter.next()
          sumexp += Math.exp(entry.getFloatValue)
        }
        Math.log(sumexp) - v.get(pidx)
    }
  }

  def grad(ymodel:TVector, yture:Double):TVector = {
    val pidx = yture.toInt
    ymodel match {
      case v: DenseDoubleVector =>
        val res = v.clone()
        var sumexp = 0.0
        res.getValues.zipWithIndex.foreach{ case (value, idx) =>
          val tmp = Math.exp(value)
          sumexp += tmp
          res.set(idx, tmp)
        }

        res.timesBy(1.0/sumexp)
        res.set(pidx,  res.get(pidx) - 1.0)
        res
      case v: DenseFloatVector =>
        val res = v.clone()
        var sumexp = 0.0f
        res.getValues.zipWithIndex.foreach{ case (value, idx) =>
          val tmp = Math.exp(value).toFloat
          sumexp += tmp
          res.set(idx, tmp)
        }
        res.timesBy(1.0/sumexp)
        res.set(pidx, res.get(pidx) - 1.0f)
        res
      case v: SparseDoubleVector =>
        val res = v.clone()
        val iter = v.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
        var sumexp = 1.0 * v.size()
        while(iter.hasNext) {
          val entry = iter.next()
          val tmp = Math.exp(entry.getDoubleValue)
          sumexp += tmp
          res.set(entry.getIntKey, tmp)
        }
        res.timesBy(1.0/sumexp)
        res.set(pidx, res.get(pidx) - 1.0)
        res
      case v: SparseFloatVector =>
        val res = v.clone()
        val iter = v.getIndexToValueMap.int2FloatEntrySet().fastIterator()
        var sumexp = 1.0 * v.size()
        while(iter.hasNext) {
          val entry = iter.next()
          val tmp = Math.exp(entry.getFloatValue).toFloat
          sumexp += tmp
          res.set(entry.getIntKey, tmp)
        }
        res.timesBy(1.0/sumexp)
        res.set(pidx,  res.get(pidx) - 1.0f)
        res
    }
  }
}
