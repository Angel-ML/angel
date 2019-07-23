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

import com.tencent.angel.spark.automl.feature.cross.FeatureCrossMeta
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.operator.{VarianceSelector, VectorCartesian, VectorFilterZero}
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class FeatureEngineeringTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test def testIterativeCross(): Unit = {

    val dim = 123
    val incDim = 123
    val iter = 3

    val data = spark.read.format("libsvm")
      .option("numFeatures", dim)
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val featureMap: Map[Int, Int] = Map[Int, Int]()

    val pipelineStages: ArrayBuffer[PipelineStage] = new ArrayBuffer
    val usedFields: ArrayBuffer[String] = new ArrayBuffer[String]()

    val cartesianPrefix = "_f"
    val selectorPrefix = "_select"
    val filterPrefix = "_filter"
    var curField = "features"
    usedFields += curField

    (0 until iter).foreach { iter =>
      val cartesian = new VectorCartesian()
      .setInputCols(Array(curField, "features"))
      .setOutputCol(curField + cartesianPrefix)
      println(s"Cartesian -> input $curField and features, output ${curField + cartesianPrefix}")
      pipelineStages += cartesian
      curField += cartesianPrefix
      val selector = new VarianceSelector()
        .setFeaturesCol(curField)
        .setOutputCol(curField + selectorPrefix)
        .setNumTopFeatures(incDim)
      println(s"Selector -> input $curField, output ${curField + selectorPrefix}")
      pipelineStages += selector
      curField += selectorPrefix
      val filter = new VectorFilterZero(featureMap)
        .setInputCol(curField)
        .setOutputCol(curField + filterPrefix)
      println(s"Filter -> input $curField, output ${curField + filterPrefix}")
      pipelineStages += filter
      curField += filterPrefix
      usedFields += curField
    }

    println(s"used fields: ${usedFields.toArray.mkString(",")}")

    val assembler = new VectorAssembler()
      .setInputCols(usedFields.toArray)
      .setOutputCol("assembled_features")
    pipelineStages += assembler

    val pipeline = new Pipeline()
      .setStages(pipelineStages.toArray)

    val crossDF = pipeline.fit(data).transform(data).persist()
    data.unpersist()
    crossDF.show(1)

//    val splitDF = crossDF.randomSplit(Array(0.7, 0.3))
//
//    val trainDF = splitDF(0).persist()
//    val testDF = splitDF(1).persist()
//
//    val originalLR = new LogisticRegression()
//      .setFeaturesCol("features")
//      .setLabelCol("label")
//      .setMaxIter(20)
//      .setRegParam(0.01)
//    val originalAUC = originalLR.fit(trainDF).evaluate(testDF)
//      .asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
//    println(s"original features auc: $originalAUC")
//
//    val crossLR = new LogisticRegression()
//      .setFeaturesCol("assembled_features")
//      .setLabelCol("label")
//      .setMaxIter(20)
//      .setRegParam(0.01)
//    val crossAUC = crossLR.fit(trainDF).evaluate(testDF)
//      .asInstanceOf[BinaryLogisticRegressionSummary].areaUnderROC
//    println(s"cross features auc: $crossAUC")
  }

}
