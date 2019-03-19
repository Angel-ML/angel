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

package com.tencent.angel.spark.automl

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.operator.{VarianceSelector, VectorCartesian}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class FeatureCrossSelectorTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test def testTwoOrderCrossAndSelector(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("f_f")

    val selector = new VarianceSelector()
      .setFeaturesCol("f_f")
      .setOutputCol("selected_f_f")
      .setNumTopFeatures(100)

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "f_f"))
      .setOutputCol("assembled_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, selector, assembler))

    val crossDF = pipeline.fit(data).transform(data).persist()
    data.unpersist()
    crossDF.drop("f_f", "selected_f_f")
    crossDF.show(1)

    val splitDF = crossDF.randomSplit(Array(0.7, 0.3))

    val trainDF = splitDF(0).persist()
    val testDF = splitDF(1).persist()

    val originalLR = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(20)
      .setRegParam(0.01)

    val originalAUC = originalLR.fit(trainDF).evaluate(testDF)
      .asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC

    println(s"original features auc: $originalAUC")

    val crossLR = new LogisticRegression()
      .setFeaturesCol("assembled_features")
      .setLabelCol("label")
      .setMaxIter(20)
      .setRegParam(0.01)

    val crossAUC = crossLR.fit(trainDF).evaluate(testDF)
      .asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC

    println(s"cross features auc: $crossAUC")
  }

}
