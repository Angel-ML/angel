package com.tencent.angel.ml.tree.utils

import com.tencent.angel.ml.math2.vector.IntFloatVector

object ProbabilisticUtils {

  /**
    * Normalize a vector of raw predictions to be a multinomial probability vector, in place.
    *
    * The input raw predictions should be nonnegative.
    * The output vector sums to 1.
    *
    * NOTE: This is NOT applicable to all models, only ones which effectively use class
    *       instance counts for raw predictions.
    *
    * @throws IllegalArgumentException if the input vector is all-0 or including negative values
    */
  def normalizeToProbabilitiesInPlace(v: IntFloatVector): Unit = {
    (0 until v.dim.toInt).foreach{  idx =>
      require(v.get(idx) >= 0,
        "The input raw predictions should be non-negative.")
    }
    val sum = v.getStorage.getValues.sum
    require(sum > 0, "Can't normalize the 0-vector.")
    var i = 0
    val size = v.size
    while (i < size) {
      v.set(i, v.get(i) / sum)
      i += 1
    }
  }
}
