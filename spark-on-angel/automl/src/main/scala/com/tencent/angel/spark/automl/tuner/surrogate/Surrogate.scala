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
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Abstract base class for surrogate model.
  *
  * @param numParams : Number of parameters in a configuration
  */
abstract class Surrogate(
                          val cs: ConfigurationSpace,
                          val minimize: Boolean = true) {

  var fields: ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
  fields += DataTypes.createStructField("label", DataTypes.DoubleType, false)
  fields += DataTypes.createStructField("features", DataTypes.createArrayType(DataTypes.DoubleType), false)

  val schema: StructType = StructType(
    StructField("label", DataTypes.DoubleType, nullable = false) ::
      StructField("features", DataTypes.createArrayType(DataTypes.DoubleType), false) ::
      Nil)

  val LOG: Log = LogFactory.getLog(classOf[Surrogate])

  // Previous input data points, (N, D)
  var preX: ArrayBuffer[Vector] = new ArrayBuffer[Vector]()
  // previous target value, (N, )
  var preY: ArrayBuffer[Double] = new ArrayBuffer[Double]()

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
  def train(X: Array[Vector], Y: Array[Double]): Unit = {
    preX.clear
    preY.clear
    preX ++ X
    preY ++ Y
    train
  }

  /**
    * Update the surrogate with more X and Y.
    *
    * @param X
    * @param Y
    */
  def update(X: Array[Vector], Y: Array[Double]): Unit = {
    if (!X.isEmpty && !Y.isEmpty) {
      X.zip(Y).foreach(tuple => print(tuple._1, tuple._2))
      preX ++= X
      preY ++= Y
      train
    }
  }

  def print(X: Vector, y: Double): Unit = {
    println(s"update surrogate with X[${X.toArray.mkString("(", ",", ")")}] " +
      s"and Y[${if (minimize) -y else y}]")
  }

  def update(X: Vector, y: Double): Unit = {
    print(X, y)
    preX += X
    preY += y
    train
  }

  /**
    * Predict means and variances for given X.
    *
    * @param X
    * @return tuples of (mean, variance)
    */
  def predict(X: Array[Vector]): Array[(Double, Double)] = {
    X.map(predict)
  }

  /**
    * Predict means and variances for a single given X.
    *
    * @param X
    * @return a tuple of (mean, variance)
    */
  def predict(X: Vector): (Double, Double)

  def stop(): Unit

  def curBest: (Vector, Double) = {
    if (minimize) curMin else curMax
  }

  def curMin: (Vector, Double) = {
    if (preY.isEmpty)
      (null, Double.MaxValue)
    else {
      val maxIdx: Int = preY.zipWithIndex.max._2
      (preX(maxIdx), -preY(maxIdx))
    }
  }

  def curMax: (Vector, Double) = {
    if (preY.isEmpty)
      (null, Double.MinValue)
    else {
      val maxIdx: Int = preY.zipWithIndex.max._2
      (preX(maxIdx), preY(maxIdx))
    }
  }
}
