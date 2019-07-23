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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.ml.linalg.Vector
import com.tencent.angel.spark.automl.tuner.config.ConfigurationSpace
import com.tencent.angel.spark.automl.tuner.kernel.Matern5Iso
import com.tencent.angel.spark.automl.tuner.model.GPModel
import com.tencent.angel.spark.automl.utils.DataUtils
import org.apache.commons.logging.{Log, LogFactory}

class GPSurrogate(
                   override val cs: ConfigurationSpace,
                   override val minimize: Boolean = true)
  extends Surrogate(cs, minimize) {

  override val LOG: Log = LogFactory.getLog(classOf[RFSurrogate])

  val covFunc = Matern5Iso()
  val initCovParams = BDV(1.0, 1.0)
  val initNoiseStdDev = 0.1
  val gpModel: GPModel = GPModel(covFunc, initCovParams, initNoiseStdDev)

  /**
    * Train the surrogate on curX and curY.
    */
  override def train(): Unit = {
    val breezeX: BDM[Double] = DataUtils.toBreeze(preX.toArray)
    val breezeY: BDV[Double] = DataUtils.toBreeze(preY.toArray)
    val success = gpModel.fit(breezeX, breezeY)
    if(!success) {
      preX.remove(preX.length - 1)
      preY.remove(preY.length - 1)
      println(s"drop the new configuration owing to convergence failure.")
    }

    /*println("Fitted covariance function params:")
    println(gpModel.covParams)
    println("Fitted noiseStdDev:")
    println(gpModel.noiseStdDev)
    println("\n")*/

  }

  /**
    * Predict means and variances for a single given X.
    *
    * @param X
    * @return a tuple of (mean, variance)
    */
  override def predict(X: Vector): (Double, Double) = {
    val breezeX = DataUtils.toBreeze(X).toDenseMatrix

    val pred = gpModel.predict(breezeX)

    //println(s"predict of ${X.toArray.mkString(",")}: mean[${pred(0, 0)}] variance[${pred(0, 1)}]")

    (pred(0, 0), pred(0, 1))
  }

  override def stop(): Unit = {

  }
}
