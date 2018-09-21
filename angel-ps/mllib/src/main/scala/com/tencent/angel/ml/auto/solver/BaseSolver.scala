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

import com.tencent.angel.ml.auto.acquisition.BaseAcquisition
import com.tencent.angel.ml.auto.maximizer.BaseMaximizer
import com.tencent.angel.ml.auto.surrogate.BaseSurrogate
import com.tencent.angel.ml.math2.vector.Vector

import scala.collection.mutable.ArrayBuffer

abstract class BaseSolver(surrogate: BaseSurrogate, acqFuc: BaseAcquisition, optimizer: BaseMaximizer, task: Runnable) {

  // Input data points, (N, D)
  var curX: ArrayBuffer[Vector]
  // Target value, (N, )
  var curY: ArrayBuffer[Double]

  def getObservations: (ArrayBuffer[Vector], ArrayBuffer[Double]) = (curX, curY)

  def getSurrogate: BaseSurrogate = surrogate


  /**
    * The main Bayesian optimization loop
    *
    * @param numIter : Number of Iterations
    * @param X       : Data points that are already evaluated
    * @param Y       : Function values of the already evaluated points
    * @return Incumbent and function value of the incumbent
    */
  def run(numIter: Int, X: Array[Vector], Y: Array[Double]): (Vector, Double)

  /**
    * Suggests a new point to evaluate.
    *
    * @param X : Data points that are already evaluated
    * @param Y : Function values of the already evaluated points
    * @return Suggested point
    */
  def chooseNext(X: Array[Vector], Y: Array[Double]): Vector
}
