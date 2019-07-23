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


package com.tencent.angel.spark.automl.tuner.surrogate

import com.tencent.angel.spark.automl.tuner.config.ConfigurationSpace
import org.apache.spark.ml.linalg.Vector


class NormalSurrogate(override val cs: ConfigurationSpace,
                      override val minimize: Boolean = true) extends Surrogate(cs, minimize) {

  override def update(X: Array[Vector], Y: Array[Double]): Unit = {
    preX ++= X
    preY ++= Y
  }

  /**
    * NormalSurrogate is designed for random-search and grid-search
    * Thus it doesn't need train and predict function
    */
  override def train(): Unit = { }


  def predict(X: Vector): (Double, Double) = { (0.0, 0.0) }

  override def stop(): Unit = { }

}