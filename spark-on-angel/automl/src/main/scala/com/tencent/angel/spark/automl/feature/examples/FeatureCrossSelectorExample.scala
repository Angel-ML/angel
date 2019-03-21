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

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.operator.{VarianceSelector, VectorCartesian}
import org.apache.spark.sql.SparkSession

object FeatureCrossSelectorExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val input = conf.get("spark.input.path", "data/a9a/a9a_123d_train_trans.libsvm")
    val numFeatures = conf.get("spark.num.feature", "123")
    val twoOrderNumFeatures = conf.getInt("spark.two.order.num.feature", 123)
    val threeOrderNumFeatures = conf.getInt("spark.three.order.num.feature", 123)

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", numFeatures)
      .load(input)
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("f_f")

    val selector = new VarianceSelector()
      .setFeaturesCol("f_f")
      .setOutputCol("selected_f_f")
      .setNumTopFeatures(twoOrderNumFeatures)

    val cartesian2 = new VectorCartesian()
      .setInputCols(Array("features", "selected_f_f"))
      .setOutputCol("f_f_f")

    val selector2 = new VarianceSelector()
      .setFeaturesCol("f_f_f")
      .setOutputCol("selected_f_f_f")
      .setNumTopFeatures(threeOrderNumFeatures)

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "selected_f_f", "selected_f_f_f"))
      .setOutputCol("assembled_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, selector, cartesian2, selector2, assembler))

    val crossDF = pipeline.fit(data).transform(data).persist()
    data.unpersist()
    crossDF.drop("f_f", "f_f_f", "selected_f_f", "selected_f_f_f")
    crossDF.show(1)

    val splitDF = crossDF.randomSplit(Array(0.9, 0.1))

    val trainDF = splitDF(0).persist()
    val testDF = splitDF(1).persist()

    val originalLR = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(20)
      .setRegParam(0.01)

    val originalPredictions = originalLR.fit(trainDF).transform(testDF)
    originalPredictions.show(1)
    val originalEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
    val originalAUC = originalEvaluator.evaluate(originalPredictions)
    println(s"original features auc: $originalAUC")

    val crossLR = new LogisticRegression()
      .setFeaturesCol("assembled_features")
      .setLabelCol("label")
      .setMaxIter(20)
      .setRegParam(0.01)

    val crossPredictions = crossLR.fit(trainDF).transform(testDF)
    crossPredictions.show(1)
    val crossEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
    val crossAUC = crossEvaluator.evaluate(crossPredictions)
    println(s"cross features auc: $crossAUC")

    spark.close()
  }
}
