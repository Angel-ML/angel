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


package com.tencent.angel.ml.auto.acquisition.optimizer

import com.tencent.angel.ml.auto.acquisition.BaseAcquisition
import com.tencent.angel.ml.auto.config.{Configuration,ConfigurationSpace}

/**
  * Abstract base class for acquisition maximization.
  * @param acqFunc     : The acquisition function which will be maximized
  * @param configSpace : Configuration space of parameters
  */
abstract class BaseOptimizer(val acqFunc: BaseAcquisition, val configSpace: ConfigurationSpace) {

  /**
    * Maximizes the given acquisition function.
    *
    * @param numPoints : Number of queried points.
    * @return A set of tuple(acquisition value, Configuration).
    */
  def maximize(numPoints: Int, sorted: Boolean = true): List[(Float, Configuration)]

  def maximize: (Float, Configuration)
}
