package com.tencent.angel.ml.optimizer2.utils.oputils

import com.tencent.angel.ml.math.vector._

class AdaDeltaExpr(var lr:Double, var rho: Float, var isInplace:Boolean) extends Ternary {

  def apply(v1: DenseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): DenseDoubleVector ={
    val result = if (isInplace) v1 else v1.clone()

    v1.getValues.zipWithIndex.foreach{ case (value, idx) =>
      var v2_last = v2.get(idx)
      if (v2_last == 0.0) v2_last = 1.0

      var v3_last = v3.get(idx)
      if (v3_last == 0.0) v3_last = 1.0
      val v3_new = v3_last * rho + (1 - rho) * value * value
      v3.set(idx, v3_new)

      val delta = - lr * Math.sqrt((v2_last+esp)/(v3_new+esp)) * value
      result.set(idx, delta)

      val v2_new = v2_last  * rho + (1 - rho) * delta * delta
      v2.set(idx, v2_new)
    }

    result
  }

  def apply(v1: SparseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): SparseDoubleVector ={
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next
      val idx = entry.getIntKey
      val value = entry.getDoubleValue

      var v2_last = v2.get(idx)
      if (v2_last == 0.0) v2_last = 1.0

      var v3_last = v3.get(idx)
      if (v3_last == 0.0) v3_last = 1.0
      val v3_new = v3_last * rho + (1 - rho) * value * value
      v3.set(idx, v3_new)

      val delta = - lr * Math.sqrt((v2_last+esp)/(v3_new+esp)) * value
      entry.setValue(delta)

      val v2_new = v2_last  * rho + (1 - rho) * delta * delta
      v2.set(idx, v2_new)
    }

    result
  }

  def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector, v3: TLongDoubleVector): SparseLongKeyDoubleVector ={
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.long2DoubleEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next
      val idx = entry.getLongKey
      val value = entry.getDoubleValue

      var v2_last = v2.get(idx)
      if (v2_last == 0.0) v2_last = 1.0

      var v3_last = v3.get(idx)
      if (v3_last == 0.0) v3_last = 1.0
      val v3_new = v3_last * rho + (1 - rho) * value * value
      v3.set(idx, v3_new)

      val delta =  - lr * Math.sqrt((v2_last+esp)/(v3_new+esp)) * value
      entry.setValue(delta)

      val v2_new = v2_last  * rho + (1 - rho) * delta * delta
      v2.set(idx, v2_new)
    }

    result
  }

  def apply(v1: DenseFloatVector, v2: TFloatVector, v3: TFloatVector): DenseFloatVector ={
    val result = if (isInplace) v1 else v1.clone()

    v1.getValues.zipWithIndex.foreach{ case (value, idx) =>
      var v2_last = v2.get(idx)
      if (v2_last == 0.0f) v2_last = 1.0f

      var v3_last = v3.get(idx)
      if (v3_last == 0.0f) v3_last = 1.0f
      val v3_new = v3_last * rho + (1 - rho) * value * value
      v3.set(idx, v3_new)

      val delta = - (lr * Math.sqrt((v2_last+esp)/(v3_new+esp)) * value).toFloat
      result.set(idx, delta)

      val v2_new = v2_last  * rho + (1 - rho) * delta * delta
      v2.set(idx, v2_new)
    }

    result
  }

  def apply(v1: SparseFloatVector, v2: TFloatVector, v3: TFloatVector): SparseFloatVector ={
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.int2FloatEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next
      val idx = entry.getIntKey
      val value = entry.getFloatValue

      var v2_last = v2.get(idx)
      if (v2_last == 0.0f) v2_last = 1.0f

      var v3_last = v3.get(idx)
      if (v3_last == 0.0f) v3_last = 1.0f
      val v3_new = v3_last * rho + (1 - rho) * value * value
      v3.set(idx, v3_new)

      val delta = - (lr * Math.sqrt((v2_last+esp)/(v3_new+esp)) * value).toFloat
      entry.setValue(delta)

      val v2_new = v2_last  * rho + (1 - rho) * delta * delta
      v2.set(idx, v2_new)
    }


    result
  }

  def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector, v3: TLongFloatVector): SparseLongKeyFloatVector ={
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.long2FloatEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next
      val idx = entry.getLongKey
      val value = entry.getFloatValue

      var v2_last = v2.get(idx)
      if (v2_last == 0.0f) v2_last = 1.0f

      var v3_last = v3.get(idx)
      if (v3_last == 0.0f) v3_last = 1.0f
      val v3_new = v3_last * rho + (1 - rho) * value * value
      v3.set(idx, v3_new)

      val delta = - (lr * Math.sqrt((v2_last+esp)/(v3_new+esp)) * value).toFloat
      entry.setValue(delta)

      val v2_new = v2_last  * rho + (1 - rho) * delta * delta
      v2.set(idx, v2_new)
    }

    result
  }

}
