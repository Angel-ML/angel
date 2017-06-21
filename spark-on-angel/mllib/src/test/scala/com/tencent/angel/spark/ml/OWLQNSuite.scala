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

package com.tencent.angel.spark.ml

import breeze.linalg.norm
import breeze.optimize.DiffFunction
import com.tencent.angel.spark.PSContext
import com.tencent.angel.spark.ml.optim.OWLQN
import com.tencent.angel.spark.models.vector.BreezePSVector

class OWLQNSuite extends PSFunSuite with SharedPSContext {

  test("super simple") {
    val dim = 3
    val capacity = 20

    val pool = PSContext.getOrCreate().createModelPool(dim, capacity)

    val l1reg = pool.createModel(1.0).mkBreeze()

    val owlqn = new OWLQN(100, 4, l1reg)

    def optimizeThis(init: BreezePSVector) = {
      val f = new DiffFunction[BreezePSVector] {
        def calculate(x: BreezePSVector) = {
          (norm((x - 3.0) :^ 2.0, 1), (x :* 2.0) - 6.0)
        }
      }

      val result = owlqn.minimize(f, init)
      result
    }

    val initWeightPS = pool.createModel(Array(-1.1053, 0.0, 0.0)).mkBreeze()
    val result = optimizeThis(initWeightPS)
    assert((result.toRemote.pull()(0) - 2.5) < 1E-4, result)
  }


  test("optimize a simple multivariate gaussian") {
    val dim = 3
    val capacity = 20

    val pool = PSContext.getOrCreate().createModelPool(dim, capacity)
    val l1reg = pool.createModel(1.0).mkBreeze()
    val lbfgs = new OWLQN(100, 4, l1reg)

    def optimizeThis(init: BreezePSVector) = {
      val f = new DiffFunction[BreezePSVector] {
        def calculate(x: BreezePSVector) = {
          (math.pow(norm(x - 3.0, 2), 2), (x :* 2.0) - 6.0)
        }
      }

      lbfgs.minimize(f, init)
    }

    val initWeightPS = pool.createModel(Array(0.0, 0.0, 0.0)).mkBreeze()
    val result = optimizeThis(initWeightPS)

    assert(norm(result - 2.5, 2) < 1E-4)
  }

}
