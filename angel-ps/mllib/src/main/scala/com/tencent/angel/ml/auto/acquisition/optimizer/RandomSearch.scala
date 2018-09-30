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
import com.tencent.angel.ml.auto.config.{Configuration, ConfigurationSpace}

import scala.util.Random

/**
  * Get candidate solutions via random sampling of configurations.
  *
  * @param acqFunc     : The acquisition function which will be maximized
  * @param configSpace : Configuration space of parameters
  * @param seed
  */
class RandomSearch(override val acqFunc: BaseAcquisition, override val configSpace: ConfigurationSpace,
                   seed: Int = 100) extends BaseOptimizer(acqFunc, configSpace) {

  val rd = new Random(seed)

  override def maximize(numPoints: Int, sorted: Boolean = true): List[(Float, Configuration)] = {
    val configs: List[Configuration] = configSpace.sampleConfig(numPoints)
    if (sorted)
      configs.map{config => (0.0f, config)}
    else
      configs.map{config => (acqFunc.compute(config.getVector)._1, config)}
  }

  override def maximize: (Float, Configuration) = {
    maximize(1, false).head
  }
}
