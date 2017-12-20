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

package com.tencent.angel.spark.context

import com.tencent.angel.spark.models.vector.enhanced.PushMan
import com.tencent.angel.spark.models.vector.{PSVector, VectorType}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class PSVectorPoolSuite extends PSFunSuite with SharedPSContext {

  test("allocate") {
    val dim = 10
    val capacity = 10
    val pool = new PSVectorPool(0, dim, capacity, VectorType.DENSE)

    var proxys: Array[PSVector] = null

    try {
      proxys = (0 until capacity).toArray.map { _ =>
        pool.allocate()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(true)
    }

    val releaseNum = 5
    proxys.slice(0, releaseNum).foreach { key =>
      pool.delete(key)
    }

    try {
      (0 until capacity - releaseNum).foreach { i =>
        pool.allocate()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(true)
    }

    proxys.slice(releaseNum, capacity).foreach { key =>
      pool.delete(key)
    }
  }

  test("auto release") {
    val dim = 10
    val capacity = 10

    val vectorArray = new Array[PSVector](capacity)
    vectorArray(0) = PSVector.dense(dim, capacity)

    (1 until capacity).foreach { i =>
      vectorArray(i) = PSVector.duplicate(vectorArray(0))
    }

    vectorArray.foreach { v =>
//      v.toCache.pullFromCache()
//      v.toCache.incrementWithCache(Array.fill(dim)(1.0))
//      v.toBreeze :+= 0.1
    }

    PushMan.flushAll()

    vectorArray.foreach { v =>
//      assert(v.pull().sameElements(Array.fill(dim)(1.1)))
    }

    (capacity / 2 until capacity).foreach { i =>
      vectorArray(i) = null
    }

    val newVector = PSVector.duplicate(vectorArray(0))
  }
}
