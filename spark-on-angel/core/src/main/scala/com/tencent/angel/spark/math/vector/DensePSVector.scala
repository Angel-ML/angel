/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

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