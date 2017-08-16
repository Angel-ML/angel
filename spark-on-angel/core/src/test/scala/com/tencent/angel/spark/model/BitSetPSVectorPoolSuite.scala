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

package com.tencent.angel.spark.model

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class BitSetPSVectorPoolSuite extends PSFunSuite with SharedPSContext {

  test("allocate") {
    val dim = 10
    val capacity = 10
    val psContext = PSContext.getOrCreate()
    val pool = psContext.createModelPool(dim, capacity).asInstanceOf[BitSetPSModelPool]

    var proxys: Array[PSModelProxy] = null

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
}
