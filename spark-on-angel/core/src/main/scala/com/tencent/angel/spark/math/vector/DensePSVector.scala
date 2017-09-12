package com.tencent.angel.spark.math.vector

import com.tencent.angel.spark.client.PSClient

class DensePSVector(override val poolId: Int,
                    override val id: Int,
                    override val dimension: Int) extends PSVector {

  def one(): DensePSVector = {
    fill(1.0)
  }

  def zero(): DensePSVector = {
    fill(0.0)
  }

  def fill(value: Double): DensePSVector = {
    PSClient.instance().fill(this, value)
    this
  }

  def fill(values: Array[Double]): DensePSVector = {
    PSClient.instance().fill(this, values)
    this
  }

  def randomUniform(min: Double, max: Double): DensePSVector = {
    PSClient.instance().randomUniform(this, min, max)
    this
  }

  def randomNormal(mean: Double, stddev: Double): DensePSVector = {
    PSClient.instance().randomNormal(this, mean, stddev)
    this
  }

}

object DensePSVector {
}