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
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.ml.feature.operator.Cartesian
import org.apache.spark.sql.SparkSession

object TwoOrderCross {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val trainDF = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")

    trainDF.persist()

    val maxDim = FeatureUtils.maxDim(trainDF)
    println(s"max dimension: $maxDim")

    // feature cross meta
    var crossInfo: Map[Int, FeatureCrossMeta] = Map[Int, FeatureCrossMeta]()
    (0 until maxDim).foreach(idx => crossInfo += idx -> FeatureCrossMeta(idx, idx.toString))

    val cartesian_1 = new Cartesian()
      .setInputCol("features")
      .setOutputCol("cartesian_features_1")

    val selector_1 = new ChiSqSelector()
      .setNumTopFeatures(maxDim * maxDim / 10)
      .setFeaturesCol("cartesian_features_1")
      .setLabelCol("label")
      .setOutputCol("selected_features_1")

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("features", "selected_features_1"))
      .setOutputCol("assemble_features_1")

    val cartesian_2 = new Cartesian()
      .setInputCol("assemble_features_1")
      .setOutputCol("cartesian_features_2")

    val selector_2 = new ChiSqSelector()
      .setNumTopFeatures(10 * maxDim)
      .setFeaturesCol("cartesian_features_2")
      .setLabelCol("label")
      .setOutputCol("selected_features_2")

    val assembler_2 = new VectorAssembler()
      .setInputCols(Array("assemble_features_1", "selected_features_2"))
      .setOutputCol("assemble_features_2")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian_1, selector_1, assembler1))
      //.setStages(Array(cartesian_1, selector_1, assembler1, cartesian_2, selector_2, assembler_2))

    val featureModel = pipeline.fit(trainDF)
    val crossDF = featureModel.transform(trainDF)

    crossDF.persist()
    trainDF.unpersist()

    println(crossDF.schema)
    crossDF.show(1)

    println("non zero features in cartesian features")
    println(FeatureUtils.countNonZero(crossDF, "cartesian_features_1").size)

    // original features
    val lr_orig = new LogisticRegression()
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.01)
    val auc_orig = lr_orig.fit(crossDF).summary.asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"original feature: auc = $auc_orig")

    // one-order cross
    val lr_cross_1 = new LogisticRegression()
      .setFeaturesCol("cartesian_features_1")
      .setMaxIter(10)
      .setRegParam(0.01)
    val auc_cross_1 = lr_cross_1.fit(crossDF).summary.asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
    println(s"one order cross feature: auc = $auc_cross_1")

    // original features + all cross features
//    val lr_cross_2 = new LogisticRegression()
//      .setFeaturesCol("assemble_features_2")
//      .setLabelCol("label")
//      .setMaxIter(10)
//      .setRegParam(0.01)
//    val auc_cross_2 = lr_cross_2.fit(crossDF).summary.asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
//    println(s"original feature + cross feature: auc = $auc_cross_2")

    spark.close()
  }

}
