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


package com.tencent.angel.spark.automl.utils

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg.{DenseMatrix => BDM}

object DataUtils {

  def parse(ss: SparkSession,
            schema: StructType,
            X: Array[Vector],
            Y: Array[Double]): DataFrame = {
    require(X.size == Y.size,
      "The size of configurations should be equal to the size of rewards.")
    ss.createDataFrame(
      Y.zip(X)).toDF("label", "features")
  }

  def parse(ss: SparkSession,
            schema: StructType,
            X: Vector): DataFrame = {
    parse(ss, schema, Array(X), Array(0))
  }

  def toBreeze(values: Array[Double]): BDV[Double] = {
    new BDV[Double](values)
  }

  def toBreeze(vector: Vector): BDV[Double] = vector match {
    case sv: SparseVector => new BDV[Double](vector.toDense.values)
    case dv: DenseVector => new BDV[Double](dv.values)
  }

  def toBreeze(X: Array[Vector]): BDM[Double] = {
    val mat = BDM.zeros[Double](X.size, X(0).size)
    for (i <- 0 until X.size) {
      for (j <- 0 until X(0).size) {
        mat(i, j) = X(i)(j)
      }
    }
    mat
  }

}
