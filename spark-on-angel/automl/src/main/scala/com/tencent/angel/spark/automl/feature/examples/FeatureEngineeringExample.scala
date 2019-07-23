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
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.operator.{VarianceSelector, VectorCartesian, VectorFilterZero}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object FeatureEngineeringExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val input = conf.get("spark.input.path", "data/a9a/a9a_123d_train_trans.libsvm")
    val numFeatures = conf.getInt("spark.num.feature", 123)
    val incNumFeatures = conf.getInt("spark.inc.num.feature", 123)
    val iter = conf.getInt("spark.ml.iteration", 1)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", numFeatures)
      .load(input)
      .persist()

    val featureMap: Map[Int, Int] = Map[Int, Int]()

    val pipelineStages: ArrayBuffer[PipelineStage] = new ArrayBuffer
    val fieldsToAssembler: ArrayBuffer[String] = new ArrayBuffer[String]()
    val allFields: ArrayBuffer[String] = new ArrayBuffer[String]()

    val cartesianPrefix = "_f"
    val selectorPrefix = "_select"
    val filterPrefix = "_filter"
    var curField = "features"
    fieldsToAssembler += curField
    allFields += curField

    (0 until iter).foreach { iter =>
      val cartesian = new VectorCartesian()
        .setInputCols(Array(curField, "features"))
        .setOutputCol(curField + cartesianPrefix)
      println(s"Cartesian -> input: $curField and features, output: ${curField + cartesianPrefix}")
      pipelineStages += cartesian
      curField += cartesianPrefix
      allFields += curField
      val selector = new VarianceSelector()
        .setFeaturesCol(curField)
        .setOutputCol(curField + selectorPrefix)
        .setNumTopFeatures(incNumFeatures)
      println(s"Selector -> input: $curField, output: ${curField + selectorPrefix}")
      pipelineStages += selector
      curField += selectorPrefix
      allFields += curField
      val filter = new VectorFilterZero(featureMap)
        .setInputCol(curField)
        .setOutputCol(curField + filterPrefix)
      println(s"Filter -> input: $curField, output: ${curField + filterPrefix}")
      pipelineStages += filter
      curField += filterPrefix
      fieldsToAssembler += curField
      allFields += curField
    }

    println(s"assembler fields: ${fieldsToAssembler.mkString(",")}")
    val assembler = new VectorAssembler()
      .setInputCols(fieldsToAssembler.toArray)
      .setOutputCol("assembled_features")
    pipelineStages += assembler
    fieldsToAssembler += "assembled_features"
    allFields += "assembled_features"

    val usedFields = Array("features", "assembled_features")
    println(s"all fields: ${allFields.toArray.mkString(",")}")
    val dropFields = allFields.filter(!usedFields.contains(_))
    println(s"drop fields: ${dropFields.toArray.mkString(",")}")

    val pipeline = new Pipeline()
      .setStages(pipelineStages.toArray)

    val crossDF = pipeline.fit(data).transform(data).persist()
    data.unpersist()
    dropFields.foreach(crossDF.drop)

    val splitDF = crossDF.randomSplit(Array(0.7, 0.3))
    val trainDF = splitDF(0).persist()
    val testDF = splitDF(1).persist()
    crossDF.unpersist()

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
  }

}
