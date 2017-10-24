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

package com.tencent.angel.spark.rdd

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}


class RDDPSFunctionsSuite extends PSFunSuite with SharedPSContext {

  test("psAggregate") {
    import RDDPSFunctions._
    val seed = 0 until 100
    val dim = 10
    val capacity = 10
    val rdd = sc.parallelize(seed, 1).map { i =>
      Array.fill[Int](dim)(i)
    }

    val remoteVector = PSVector.dense(dim, capacity).toCache

    def seqOp: (Int, Array[Int]) => Int = { (c: Int, x: Array[Int]) =>
      remoteVector.incrementWithCache(x.map(_.toDouble))
      c + 1
    }
    def combOp: (Int, Int) => Int = (c1: Int, c2: Int) => c1 + c2
    val count = rdd.psAggregate(0)(seqOp, combOp)

    val result = Array.fill[Int](dim)(seed.sum)
    assert(count === seed.length)
    assert(remoteVector.pullFromCache().map(_.toInt).sameElements(result))

    PSContext.instance().destroyVectorPool(remoteVector)
  }

  test("psFoldLeft") {
    import RDDPSFunctions._

    val seed = 0 until 100
    val dim = 10
    val capacity = 10
    val rdd = sc.parallelize(seed, 1).map { i =>
      Array.fill[Int](dim)(i)
    }

    val remoteVector = PSVector.dense(dim, capacity).fill(Double.NegativeInfinity).toCache

    val max = rdd.psFoldLeft(remoteVector) { (pv, bv) =>
      pv.mergeMaxWithCache(bv.map(_.toDouble))
      pv
    }

    assert(max.pullFromCache().map(_.toInt).sameElements(Array.fill(dim)(99)))

    PSContext.instance().destroyVectorPool(remoteVector)
  }
}
