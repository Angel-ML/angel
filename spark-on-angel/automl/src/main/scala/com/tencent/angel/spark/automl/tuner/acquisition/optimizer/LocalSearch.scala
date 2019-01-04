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


package com.tencent.angel.spark.automl.tuner.acquisition.optimizer

import com.tencent.angel.spark.automl.tuner.acquisition.Acquisition
import com.tencent.angel.spark.automl.tuner.config.{Configuration, ConfigurationSpace}

/**
  * Implementation of local search.
  *
  * @param acqFunc     : The acquisition function which will be maximized
  * @param configSpace : Configuration space of parameters
  * @param epsilon     : In order to perform a local move one of the incumbent's neighbors needs at least an improvement higher than epsilon
  * @param numIters    : Maximum number of iterations that the local search will perform
  */
class LocalSearch(
                   override val acqFunc: Acquisition,
                   override val configSpace: ConfigurationSpace,
                   epsilon: String, numIters: Int)
  extends AcqOptimizer(acqFunc, configSpace) {

  /**
    * Starts a local search from the given start point and quits if either the max number of steps is reached or
    * no neighbor with an higher improvement was found
    *
    * @param numPoints : Number of queried points.
    * @return A set of tuple(acquisition_value, Configuration).
    */
  override def maximize(numPoints: Int,
                        sorted: Boolean = true): Array[(Double, Configuration)] = ???

  override def maximize: (Double, Configuration) = ???
}
