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
 */

package com.tencent.angel.spark.ops

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class InitializerSuite extends PSFunSuite with SharedPSContext {

  private val dim = 14
  private val capacity = 12
  private var _initOps: Initializer = _
  var psVector: DensePSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // start Angel
    _initOps = PSClient.instance().initOps

    // create pool
    psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _initOps = null
    super.afterAll()
  }

  test("doFill") {
    val psProxy = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)

    _initOps.fill(psProxy, 3.14)
    assert(psProxy.pull().sameElements(Array.fill[Double](dim)(3.14)))
  }

  test("doRandomUniform") {
    val psProxy = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)
    _initOps.randomUniform(psProxy, -1.0, 1.0)
    psProxy.pull().foreach { x =>
      assert(x < 1.0 && x > -1.0)
    }
  }

  /** Todo: a bug in Angel.
   * test("doRandomNormal") {
   * val pool = _vectorOps.createVectorPool(10000, 2)
   * val psKey = pool.initModel()
   * _vectorOps.doRandomNormal(psKey, 0.0, 1.0)

   * val localArray = psKey.toLocal().get()

   * val mean = localArray.sum / localArray.length
   * val variety = localArray.map(x => SMath.pow(x - mean, 2.0)).sum / (localArray.length - 1)

   * val tol = 0.1
   * assert(SMath.abs(mean - 0.0) < tol)
   * assert(SMath.abs(SMath.sqrt(variety) - 1.0) < tol)

   * _vectorOps.destroyVectorPool(pool)
   * }
   **/
}
