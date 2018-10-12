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

import com.tencent.angel.ml.auto.acquisition.Acquisition
import com.tencent.angel.ml.auto.acquisition.optimizer.AcqOptimizer
import com.tencent.angel.ml.auto.config.{Configuration, ConfigurationSpace}
import com.tencent.angel.ml.auto.setting.Setting
import com.tencent.angel.ml.auto.surrogate.Surrogate
import com.tencent.angel.ml.math2.vector.IntFloatVector

class Solver(val cs: ConfigurationSpace, val surrogate: Surrogate, val acqFuc: Acquisition, val optimizer: AcqOptimizer) {

  def getObservations: (List[IntFloatVector], List[Float]) = (surrogate.curX.toList, surrogate.curY.toList)

  def getSurrogate: Surrogate = surrogate

  /**
    * Suggests configurations to evaluate.
    */
  def suggest(): List[Configuration] = {
    optimizer.maximize(Setting.batchSize).map(_._2)
  }

  /**
    * Feed evaluation result to the model
    * @param configs: More evaluated configurations
    * @param Y: More evaluation result
    */
  def feed(configs: List[Configuration], Y: List[Float]): Unit = {
    surrogate.update(configs.map(_.getVector), Y)
  }

  def feed(config: Configuration, y: Float): Unit = {
    surrogate.update(config.getVector, y)
  }
}
