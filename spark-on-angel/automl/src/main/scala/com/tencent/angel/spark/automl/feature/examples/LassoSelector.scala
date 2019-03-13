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

package com.tencent.angel.spark.automl.feature.examples

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.operator.Cartesian
import org.apache.spark.sql.SparkSession

object LassoSelector {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val splitData = data.randomSplit(Array(0.7, 0.3))

    val trainDF = splitData(0)
    val testDF = splitData(1)

    val cartesianOp = new Cartesian()
      .setInputCol("features")
      .setOutputCol("cartesian_features")

    val crossDF = cartesianOp.transform(trainDF)

    val lr = new LogisticRegression()
      .setFeaturesCol("cartesian_features")
      .setLabelCol("label")
      .setElasticNetParam(1.0)
      .setMaxIter(10)

    val lrModel = lr.fit(crossDF)
    lrModel.evaluate(testDF)

    println(s"nonzero items in weight vector:")
    println(lrModel.coefficients.toDense.values.zipWithIndex
      .filter { case (v: Double, i: Int) => math.abs(v) > 1e-10 }.take(100).mkString(","))
  }

}
