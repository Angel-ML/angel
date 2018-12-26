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


package com.tencent.angel.spark.ml.automl.tuner.solver

import com.tencent.angel.spark.ml.automl.tuner.TunerParam
import com.tencent.angel.spark.ml.automl.tuner.acquisition.Acquisition
import com.tencent.angel.spark.ml.automl.tuner.acquisition.optimizer.AcqOptimizer
import com.tencent.angel.spark.ml.automl.tuner.config.{Configuration, ConfigurationSpace}
import com.tencent.angel.spark.ml.automl.tuner.surrogate.Surrogate
import org.apache.spark.ml.linalg.Vector
import org.apache.commons.logging.{Log, LogFactory}

class Solver(
              val cs: ConfigurationSpace,
              val surrogate: Surrogate,
              val acqFuc: Acquisition,
              val optimizer: AcqOptimizer) {

  val LOG: Log = LogFactory.getLog(classOf[Solver])

  def getObservations(): (Array[Vector], Array[Double]) = (surrogate.curX.toArray, surrogate.curY.toArray)

  def getSurrogate: Surrogate = surrogate

  /**
    * Suggests configurations to evaluate.
    */
  def suggest(): Array[Configuration] = {
    //println(s"suggest configurations")
    optimizer.maximize(TunerParam.batchSize).map(_._2)
  }

  /**
    * Feed evaluation result to the model
    * @param configs: More evaluated configurations
    * @param Y: More evaluation result
    */
  def feed(configs: Array[Configuration], Y: Array[Double]): Unit = {
    //println(s"feed ${configs.size} configurations")
    surrogate.update(configs.map(_.getVector), Y)
  }

  def feed(config: Configuration, y: Double): Unit = {
    surrogate.update(config.getVector, y)
  }
}
