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

import com.tencent.angel.spark.automl.feature.FeatureUtils
import com.tencent.angel.spark.automl.feature.cross.FeatureCrossMeta
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.operator.Cartesian
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.sql.SparkSession

object OneOrderCross {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val maxDim = FeatureUtils.maxDim(data)
    println(s"max dimension: $maxDim")

    // feature cross meta
    var crossInfo: Map[Int, FeatureCrossMeta] = Map[Int, FeatureCrossMeta]()
    (0 until maxDim).foreach(idx => crossInfo += idx -> FeatureCrossMeta(idx, idx.toString))

    val cartesianOp = new Cartesian()
      .setInputCol("features")
      .setOutputCol("cartesian_features")

    //val crossDF = cartesianOp.transform(trainDF).persist()

    val selector = new ChiSqSelector()
      .setNumTopFeatures(maxDim * maxDim / 2)
      .setFeaturesCol("cartesian_features")
      .setLabelCol("label")
      .setOutputCol("selected_features")

    val allAssembler = new VectorAssembler()
      .setInputCols(Array("features", "cartesian_features"))
      .setOutputCol("assemble_features")

    val selectedAssembler = new VectorAssembler()
      .setInputCols(Array("features", "selected_features"))
      .setOutputCol("assemble_features_selected")

    val pipeline = new Pipeline()
      .setStages(Array(cartesianOp, selector, allAssembler, selectedAssembler))

    val featureModel = pipeline.fit(data)
    val crossDF = featureModel.transform(data)

    println(crossDF.schema)
    crossDF.show(1)

    val splitData = crossDF.randomSplit(Array(0.7, 0.3))

    val trainDF = splitData(0).persist()
    val testDF = splitData(1).persist()

    // original features
    val lr_orig = new LogisticRegression()
        .setFeaturesCol("features")
        .setMaxIter(10)
        .setRegParam(0.01)
    val auc_orig = lr_orig.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $auc_orig")

    // original features
    val lr_cross = new LogisticRegression()
      .setFeaturesCol("cartesian_features")
      .setMaxIter(10)
      .setRegParam(0.01)
    val auc_cross = lr_cross.fit(trainDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"cross feature: auc = $auc_cross")

    // original features + all cross features
    val lr_orig_cross = new LogisticRegression()
      .setFeaturesCol("assemble_features")
      .setLabelCol("label")
      .setMaxIter(10)
      .setRegParam(0.01)
    val auc_orig_cross = lr_orig_cross.fit(crossDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature + cross feature: auc = $auc_orig_cross")

    // original features + selected cross features
    val lr_orig_cross_select = new LogisticRegression()
      .setFeaturesCol("assemble_features_selected")
      .setLabelCol("label")
      .setMaxIter(10)
      .setRegParam(0.01)
    val auc_orig_cross_select = lr_orig_cross_select.fit(crossDF).evaluate(testDF).asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature + selected cross feature: auc = $auc_orig_cross_select")


    spark.close()
  }

}
