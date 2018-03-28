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

import scala.util.Random

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class DenseRowOpsSuite extends PSFunSuite with SharedPSContext {

  private val dim = 14
  private val capacity = 12
  private var _denseRowOps: DenseRowOps = _
  var psVector: DensePSVector = _
  var zeroVector: DensePSVector = _
  var uniformVector: DensePSVector = _
  var normalVector: DensePSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // start Angel
    _denseRowOps = PSClient.instance().denseRowOps
    psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _denseRowOps = null
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    psVector.fill(0.0)
    zeroVector = PSVector.duplicate(psVector)
    uniformVector = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)
    normalVector = PSVector.duplicate(psVector).randomNormal(0.0, 1.0)
  }

  override def afterEach(): Unit = {
    normalVector.delete()
    uniformVector.delete()
    zeroVector.delete()
    super.afterEach()
  }

  test("dense pull") {
    val thisVector = PSVector.duplicate(psVector).fill(3.14)

    thisVector.pull.values.foreach { v =>
      assert(v == 3.14)
    }
  }

  test("doRandomUniform") {
    val psProxy = PSVector.duplicate(psVector).randomUniform(0.0, 1.0)
    _denseRowOps.randomUniform(psProxy, -1.0, 1.0)
    psProxy.pull.values.foreach { x =>
      assert(x < 1.0 && x > -1.0)
    }
  }

  /** Todo: a bug in Angel.
   * test("doRandomNormal") {
   * val pool = _denseRowOps.createVectorPool(10000, 2)
   * val psKey = pool.initModel()
   * _denseRowOps.doRandomNormal(psKey, 0.0, 1.0)

   * val localArray = psKey.toLocal().get()

   * val mean = localArray.sum / localArray.length
   * val variety = localArray.map(x => SMath.pow(x - mean, 2.0)).sum / (localArray.length - 1)

   * val tol = 0.1
   * assert(SMath.abs(mean - 0.0) < tol)
   * assert(SMath.abs(SMath.sqrt(variety) - 1.0) < tol)

   * _denseRowOps.destroyVectorPool(pool)
   * }
   **/

  test("push") {
    val rand = new Random()
    val localVector = (0 until dim).toArray.map(_ => rand.nextDouble())

    _denseRowOps.push(psVector, localVector)
    assert(localVector.sameElements(psVector.pull.values))
  }

  test("increment dense") {
    val localArray = Array.fill[Double](dim)(0.1)

    val oldPSArray = normalVector.pull.values
    val result = localArray.indices.map(i => oldPSArray(i) + localArray(i))

    _denseRowOps.increment(normalVector, localArray)

    assert(normalVector.pull.values.sameElements(result))
  }
  
  test("doMergeMax") {
    val localPSVector = normalVector.pull.values

    val localArray = Array.fill[Double](dim)(0.1)
    _denseRowOps.mergeMax(normalVector, localArray)

    val max = localArray.indices.map { i =>
      if (localArray(i) > localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalVector.pull.values.sameElements(max))
  }

  test("doMergeMin") {
    val localPSVector = normalVector.pull.values

    val localArray = Array.fill[Double](dim)(0.1)
    _denseRowOps.mergeMin(normalVector, localArray)

    val min = localArray.indices.map { i =>
      if (localArray(i) < localPSVector(i)) localArray(i) else localPSVector(i)
    }

    assert(normalVector.pull.values.sameElements(min))
  }

}
