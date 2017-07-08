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

package com.tencent.angel.spark.models

import com.tencent.angel.spark.{PSContext, PSFunSuite, SharedPSContext}
import org.scalatest.Ignore

import scala.util.Random

class PSVectorPoolSuite extends PSFunSuite with SharedPSContext {

  val dim = 10
  val capacity = 5
  private var _psContext: PSContext = _
  private var _pool: PSModelPool = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // PS setup
    _psContext = PSContext.getOrCreate()
    _pool = _psContext.createModelPool(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psContext.destroyModelPool(_pool)
    super.afterAll()
  }

  test("create") {
    val rand = new Random(42)
    val array = (0 until dim).toArray.map(i => rand.nextGaussian())
    val proxy = _pool.createModel(array)

    assert(proxy.mkRemote().pull().sameElements(array))
  }

  test("fill") {
    val proxy = _pool.createModel(3.1415)
    assert(proxy.mkRemote().pull().sameElements(Array.fill(dim)(3.1415)))
  }

  test("randomUniform") {
    val proxy = _pool.createRandomUniform(0.0, 1.0)

    var isCorrect = true
    proxy.mkRemote().pull().foreach(x => if (x < 0.0 || x > 1.0) isCorrect = false )
    assert(isCorrect)
  }

  test("randomNormal") {
    val pool = _psContext.createModelPool(10000, 2)
    val proxy = pool.createRandomNormal(0.0, 1.0)

    val array = proxy.mkRemote().pull()
    val mean = array.sum / array.length
    val variety = array.map(x => math.pow(x - mean, 2.0)).sum / (array.length - 1)

    val tol = 0.1
    assert(math.abs(mean - 0.0) < tol)
    assert(math.abs(math.sqrt(variety) - 1.0) < tol)
  }

}
