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
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


class NormalSurrogate(
                   val cs: ConfigurationSpace,
                   val minimize: Boolean = true) {

  var fields: ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
  fields += DataTypes.createStructField("label", DataTypes.DoubleType, false)
  fields += DataTypes.createStructField("features", DataTypes.createArrayType(DataTypes.DoubleType), false)

  val schema: StructType = StructType(
    StructField("label", DataTypes.DoubleType, nullable = false) ::
      StructField("features",  DataTypes.createArrayType(DataTypes.DoubleType), false) ::
      Nil)

  val LOG: Log = LogFactory.getLog(classOf[Surrogate])

  // Previous input data points, (N, D)
  var preX: ArrayBuffer[Vector] = new ArrayBuffer[Vector]()
  // previous target value, (N, )
  var preY: ArrayBuffer[Double] = new ArrayBuffer[Double]()


  def update(X: Array[Vector], Y: Array[Double]): Unit = {
    preX ++= X
    preY ++= Y
  }

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