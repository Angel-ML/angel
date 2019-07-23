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


package com.tencent.angel.spark.automl.tuner.acquisition

import com.tencent.angel.spark.automl.tuner.surrogate.Surrogate
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Expected improvement.
  *
  * @param surrogate
  * @param beta : Controls the upper conﬁdence bound
  *             Assume :
  *             - t: number of iteration
  * 	        	- d: dimension of optimization space
  * 	          - v: hyperparameter v = 1
  * 		        - delta: small constant 0.1 (prob of regret)
  *             Suggest value：beta = sqrt( v* (2*  log( (t**(d/2. + 2))*(pi**2)/(3. * delta)  )))
 */
class UCB(
          override val surrogate: Surrogate,
          val beta: Double = 100)
  extends Acquisition(surrogate) {

  val LOG: Log = LogFactory.getLog(classOf[Surrogate])

  override def compute(X: Vector, derivative: Boolean = false): (Double, Vector) = {
    val pred = surrogate.predict(X) // (mean, variance)

    val m: Double = pred._1
    val s: Double = Math.sqrt(pred._2)

    if (s == 0) {
      // if std is zero, we have observed x on all instances
      // using a RF, std should be never exactly 0.0
      (0.0, Vectors.dense(new Array[Double](X.size)))
    } else {
      val ucb = m + beta*s

      (ucb, Vectors.dense(new Array[Double](X.size)))
    }
  }
}