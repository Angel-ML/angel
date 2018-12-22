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


package com.tencent.angel.spark.ml.automl.tuner.surrogate

import com.tencent.angel.spark.ml.automl.tuner.config.ConfigurationSpace
import org.apache.spark.ml.linalg.{DenseMatrix, Vector}
import com.tencent.angel.spark.ml.automl.utils.DataUtils
import org.apache.commons.logging.{Log, LogFactory}

class GPSurrogate(
                   override val cs: ConfigurationSpace,
                   override val minimize: Boolean = true)
  extends Surrogate(cs, minimize) {

  override val LOG: Log = LogFactory.getLog(classOf[RFSurrogate])


  /**
    * Train the surrogate on curX and curY.
    */
  override def train(): Unit = {

    val breezeX = DataUtils.toBreeze(curX.toArray)
    val breezeY = DataUtils.toBreeze(curY.toArray)

  }

  /**
    * Predict means and variances for a single given X.
    *
    * @param X
    * @return a tuple of (mean, variance)
    */
  override def predict(X: Vector): (Double, Double) = ???

  override def stop(): Unit = ???
}
