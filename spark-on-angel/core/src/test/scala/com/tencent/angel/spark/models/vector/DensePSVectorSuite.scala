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

package com.tencent.angel.spark.models.vector

import scala.util.Random

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.DenseVector
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class DensePSVectorSuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psContext: PSContext = _
  private var _psVector: DensePSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.instance()
    _psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psContext.destroyVectorPool(_psVector)
    super.afterAll()
  }

  test("fill with value") {
    val dVector = PSVector.duplicate(_psVector).fill(3.14)

    dVector.pull.values.foreach { element =>
      assert(element == 3.14)
    }
  }

  test("fill with array") {
    val rand = new Random()
    val localArray = (0 until dim).toArray.map { i =>
      rand.nextDouble()
    }
    val dVector = PSVector.duplicate(_psVector).push(new DenseVector(localArray))

    val remoteArray = dVector.pull

    (0 until dim).foreach { index =>
      assert(math.abs(remoteArray(index) - localArray(index)) < 1e-6)
    }
  }

  test("randomUniform") {
    val dVector = PSVector.duplicate(_psVector).randomUniform(0.0, 1.0)

    var isCorrect = true
    dVector.pull.values.foreach(x => if (x < 0.0 || x > 1.0) isCorrect = false )
    assert(isCorrect)
  }

  test("randomNormal") {

    val dVector = PSVector.dense(10000, 2).randomNormal(0.0, 1.0)

    val array = dVector.pull.values
    val mean = array.sum / array.length
    val variety = array.map(x => math.pow(x - mean, 2.0)).sum / (array.length - 1)

    val tol = 0.1
    assert(math.abs(mean - 0.0) < tol)
    assert(math.abs(math.sqrt(variety) - 1.0) < tol)
  }
}
