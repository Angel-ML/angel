package com.tencent.angel.ml.optimizer2.utils.oputils

import com.tencent.angel.ml.math.vector._

class AdamExpr(var iterNum:Int, var lr:Double, var rho: Float, var phi:Float, var isInplace:Boolean) extends Ternary {
  private val (rho_coeff, phi_coeff) = (1.0 - Math.pow(rho, iterNum).toFloat, 1.0 - Math.pow(phi, iterNum).toFloat)

  def apply(v1: DenseDoubleVector, v2: TDoubleVector, v3: TDoubleVector): DenseDoubleVector ={
    val result = if (isInplace) v1 else v1.clone()

    v1.getValues.zipWithIndex.foreach{ case (value, idx) =>
      val v2_last = v2.get(idx)
      val v2_new = v2_last  * rho + (1 - rho) * value
      v2.set(idx, v2_new)

      val v3_last = v3.get(idx)
      val v3_new = v3_last * phi + (1 - phi) * value * value
      v3.set(idx, v3_new)

      val delta = - lr * clip((v2_new/rho_coeff) / Math.sqrt(v3_new/phi_coeff + esp))
      result.set(idx, delta)
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

      if (value == 0.0) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = v2_last  * rho + (1 - rho) * value
        v2.set(idx, v2_new)

        val v3_last = v3.get(idx)
        val v3_new = v3_last * phi + (1 - phi) * value * value
        v3.set(idx, v3_new)

        val delta = - lr * clip((v2_new/rho_coeff) / Math.sqrt(v3_new/phi_coeff + esp))
        entry.setValue(delta)
      }
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

      if (value == 0.0) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = v2_last * rho + (1 - rho) * value
        v2.set(idx, v2_new)

        val v3_last = v3.get(idx)
        val v3_new = v3_last * phi + (1 - phi) * value * value
        v3.set(idx, v3_new)

        val delta = -lr * clip((v2_new / rho_coeff) / Math.sqrt(v3_new / phi_coeff + esp))
        entry.setValue(delta)
      }
    }

    result
  }

  def apply(v1: DenseFloatVector, v2: TFloatVector, v3: TFloatVector): DenseFloatVector ={
    val result = if (isInplace) v1 else v1.clone()

    v1.getValues.zipWithIndex.foreach{ case (value, idx) =>
      val v2_last = v2.get(idx)
      val v2_new = v2_last  * rho + (1 - rho) * value
      v2.set(idx, v2_new)

      val v3_last = v3.get(idx)
      val v3_new = v3_last * phi + (1 - phi) * value * value
      v3.set(idx, v3_new)

      val delta = - lr * clip((v2_new/rho_coeff) / Math.sqrt(v3_new/phi_coeff + esp))
      result.set(idx, delta.toFloat)
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

      if (value == 0.0f) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = v2_last * rho + (1 - rho) * value
        v2.set(idx, v2_new)

        val v3_last = v3.get(idx)
        val v3_new = v3_last * phi + (1 - phi) * value * value
        v3.set(idx, v3_new)

        val delta = -lr * clip((v2_new / rho_coeff) / Math.sqrt(v3_new / phi_coeff + esp))
        entry.setValue(delta.toFloat)
      }
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

      if (value == 0.0f) {
        iter.remove()
      } else {
        val v2_last = v2.get(idx)
        val v2_new = v2_last * rho + (1 - rho) * value
        v2.set(idx, v2_new)

        val v3_last = v3.get(idx)
        val v3_new = v3_last * phi + (1 - phi) * value * value
        v3.set(idx, v3_new)

        val delta = -lr * clip((v2_new / rho_coeff) / Math.sqrt(v3_new / phi_coeff + esp))
        entry.setValue(delta.toFloat)
      }
    }

    result
  }

}