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

package com.tencent.angel.ml.core.optimizer.decayer

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}

import scala.reflect.ClassTag

trait StepSizeScheduler extends Serializable {

  def next(): Double

  def isIntervalBoundary: Boolean = false

}

object StepSizeScheduler {

  private def matchName[T: ClassTag](name: String): Boolean = {
    val cls = implicitly[ClassTag[T]].runtimeClass
    cls.getSimpleName.equalsIgnoreCase(name)
  }

  def apply(name: String, eta: Double): StepSizeScheduler = {
    val conf = SharedConf.get()
    name match {
      case clsName if matchName[StandardDecay](clsName) =>
        val alpha = conf.getDouble(MLConf.ML_LEARN_DECAY,
          conf.getDouble(MLConf.ML_OPT_DECAY_ALPHA, MLConf.DEFAULT_ML_LEARN_DECAY))
        new StandardDecay(eta, alpha)
      case clsName if matchName[WarmRestarts](clsName) =>
        val etaMin = eta / 10
        val alpha = conf.getDouble(MLConf.ML_OPT_DECAY_ALPHA, MLConf.DEFAULT_ML_OPT_DECAY_ALPHA)
        new WarmRestarts(eta, etaMin, alpha)
      case clsName if matchName[CorrectionDecay](clsName) =>
        val alpha = conf.getDouble(MLConf.ML_OPT_DECAY_ALPHA, MLConf.DEFAULT_ML_OPT_DECAY_ALPHA)
        val beta = conf.getDouble(MLConf.ML_OPT_DECAY_BETA, MLConf.DEFAULT_ML_OPT_DECAY_BETA)
        new CorrectionDecay(eta, alpha, beta)
      case clsName if matchName[ConstantLearningRate](clsName) =>
        new ConstantLearningRate(eta)
    }
  }
}
