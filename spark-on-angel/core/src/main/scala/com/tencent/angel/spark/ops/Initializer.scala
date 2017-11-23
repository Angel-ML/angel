package com.tencent.angel.spark.ops

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.matrix.PSMatrix
import com.tencent.angel.spark.models.vector.PSVector

class Initializer {

  /**
    * Generate a random PSVector, the random distribution is uniform.
    * Notice: it can only be called in th driver.
    *
    * @param min the minimum of uniform distribution
    * @param max the maximum of uniform distribution
    */
  def randomUniform(to: PSVector, min: Double, max: Double): Unit = {
    to.assertValid()
    update(to.poolId, new RandomUniform(to.poolId, to.id, min, max))
  }

  /**
    * Generate a random PSVector, the random distribution is normal distribution.
    * Notice: it can only be called in th driver.
    *
    * @param mean the `mean` parameter of uniform distribution
    * @param stddev the `stddev` parameter of uniform distribution
    */
  def randomNormal(to: PSVector, mean: Double, stddev: Double): Unit = {
    to.assertValid()
    update(to.poolId, new RandomNormal(to.poolId, to.id, mean, stddev))
  }

  private def update(modelId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  /**
    * Initialize a random matrix, whose value is a random(0.0, 1.0)
    */
  def random(mat: PSMatrix): Unit = {
    mat.assertValid()
    update(mat.meta.getId, new Random(mat.meta.getId))
  }

  /**
    * Fill PSVectorKey with `value`
    * Notice: it can only be called in th driver.
    */
  def fill(to: PSVector, value: Double): Unit = {
    to.assertValid()
    update(to.poolId, new Fill(to.poolId, to.id, value))
  }

  def fill(to: PSVector, values: Array[Double]): Unit = {
    to.assertValid()
    update(to.poolId, new Push(to.poolId, to.id, values))
  }


  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }
}
