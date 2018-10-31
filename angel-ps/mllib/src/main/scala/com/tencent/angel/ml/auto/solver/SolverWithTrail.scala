/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.auto.solver

import com.tencent.angel.ml.auto.config.Configuration
import com.tencent.angel.ml.auto.trail.Trail
import com.tencent.angel.ml.math2.vector.IntFloatVector

class SolverWithTrail(val solver: Solver, val trail: Trail) {

  /**
    * The main Bayesian optimization loop
    *
    * @param numIter : Number of Iterations
    * @param X       : Initial data points that are already evaluated
    * @param Y       : Initial function values of the already evaluated points
    * @return Incumbent and function value of the incumbent
    */
  def run(numIter: Int, X: List[Configuration] = Nil, Y: List[Float] = Nil): (IntFloatVector, Float) = {
    if (X != Nil && Y != Nil && X.size == Y.size) solver.feed(X, Y)
    (0 until numIter).foreach{ iter =>
      println(s"------iteration $iter starts------")
      val configs: List[Configuration] = solver.suggest
      val results: List[Float] = trail.evaluate(configs)
      solver.feed(configs, results)
    }
    solver.surrogate.curBest
  }
}
