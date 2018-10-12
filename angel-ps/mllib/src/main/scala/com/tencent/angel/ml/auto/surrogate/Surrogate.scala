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


package com.tencent.angel.ml.auto.surrogate

import com.tencent.angel.ml.math2.vector.IntFloatVector

import scala.collection.mutable.ListBuffer

/**
  * Abstract base class for surrogate model.
  * @param numParams : Number of parameters in a configuration
  */
abstract class Surrogate(val numParams: Int, val minimize: Boolean = true) {
  // Input data points, (N, D)
  var curX: ListBuffer[IntFloatVector] = new ListBuffer[IntFloatVector]()
  // Target value, (N, )
  var curY: ListBuffer[Float] = new ListBuffer[Float]()

  /**
    * Train the surrogate on curX and curY.
    */
  def train(): Unit

  /**
    * Train the surrogate on X and Y.
    *
    * @param X : (N, D), input data points.
    * @param Y : (N, 1), the corresponding target values.
    */
  def train(X: List[IntFloatVector], Y: List[Float]): Unit = {
    curX.clear
    curY.clear
    curX ++ X
    curY ++ Y
    train
  }

  /**
    * Update the surrogate with more X and Y.
    *
    * @param X
    * @param Y
    */
  def update(X: List[IntFloatVector], Y: List[Float]): Unit = {
    curX ++ X
    curY ++ Y
    train
  }

  def update(X: IntFloatVector, y: Float): Unit = {
    curX += X
    curY += y
    train
  }

  /**
    * Predict means and variances for given X.
    *
    * @param X
    * @return tuples of (mean, variance)
    */
  def predict(X: List[IntFloatVector]): List[(Float, Float)] = {
    X.map(predict)
  }

  /**
    * Predict means and variances for a single given X.
    *
    * @param X
    * @return a tuple of (mean, variance)
    */
  def predict(X: IntFloatVector): (Float, Float)

  def curBest: (IntFloatVector, Float) = {
    if (minimize) curMin else curMax
  }

  def curMin: (IntFloatVector, Float) = {
    val minIdx: Int = curY.zipWithIndex.min._2
    (curX(minIdx), curY(minIdx))
  }

  def curMax: (IntFloatVector, Float) = {
    val maxIdx: Int = curY.zipWithIndex.max._2
    (curX(maxIdx), curY(maxIdx))
  }
}
