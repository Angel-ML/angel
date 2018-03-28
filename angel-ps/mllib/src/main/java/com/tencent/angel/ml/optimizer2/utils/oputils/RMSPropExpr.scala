package com.tencent.angel.ml.optimizer2.utils.oputils

import com.tencent.angel.ml.math.vector._

class RMSPropExpr(val lr:Double, var rho:Float, var isInplace:Boolean) extends Binary {
  override def apply(v1: DenseDoubleVector, v2: TDoubleVector): DenseDoubleVector = {
    val result = if (isInplace) v1 else v1.clone()

    v1.getValues.zipWithIndex.foreach{ case (value, idx) =>
      val v2_last = v2.get(idx)
      val v2_new = rho * v2_last + (1 - rho) * value * value
      v2.set(idx, v2_new)

      val delta = - lr / clip(Math.sqrt(v2_new + esp)) * value

      result.set(idx, delta)
    }

    result
  }

  override def apply(v1: SparseDoubleVector, v2: TDoubleVector): SparseDoubleVector = {
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.int2DoubleEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val idx = entry.getIntKey
      val value = entry.getDoubleValue

      if (value == 0.0) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = rho * v2_last + (1 - rho) * value * value
        v2.set(idx, v2_new)

        val delta = - lr / clip(Math.sqrt(v2_new + esp)) * value
        entry.setValue(delta)
      }
    }

    result
  }

  override def apply(v1: SparseLongKeyDoubleVector, v2: TLongDoubleVector): SparseLongKeyDoubleVector = {
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.long2DoubleEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val idx = entry.getLongKey
      val value = entry.getDoubleValue

      if (value == 0.0) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = rho * v2_last + (1 - rho) * value * value
        v2.set(idx, v2_new)

        val delta = -lr / clip(Math.sqrt(v2_new + esp)) * value
        entry.setValue(delta)
      }
    }

    result
  }

  override def apply(v1: DenseFloatVector, v2: TFloatVector): DenseFloatVector = {
    val result = if (isInplace) v1 else v1.clone()

    v1.getValues.zipWithIndex.foreach{ case (value, idx) =>
      val v2_last = v2.get(idx)
      val v2_new = rho * v2_last + (1 - rho) * value * value
      v2.set(idx, v2_new)

      val delta = - lr / clip(Math.sqrt(v2_new + esp)) * value
      result.set(idx, delta.toFloat)
    }

    result
  }

  override def apply(v1: SparseFloatVector, v2: TFloatVector): SparseFloatVector = {
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.int2FloatEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val idx = entry.getIntKey
      val value = entry.getFloatValue

      if (value == 0.0f) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = rho * v2_last + (1 - rho) * value * value
        v2.set(idx, v2_new)

        val delta = -lr / clip(Math.sqrt(v2_new + esp)) * value
        entry.setValue(delta.toFloat)
      }
    }

    result
  }

  override def apply(v1: SparseLongKeyFloatVector, v2: TLongFloatVector): SparseLongKeyFloatVector = {
    val result = if (isInplace) v1 else v1.clone()

    val iter = result.getIndexToValueMap.long2FloatEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val idx = entry.getLongKey
      val value = entry.getFloatValue

      if (value == 0.0f) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = rho * v2_last + (1 - rho) * value * value
        v2.set(idx, v2_new)

        val delta = -lr / clip(Math.sqrt(v2_new + esp)) * value
        entry.setValue(delta.toFloat)
      }
    }

    result
  }
}
