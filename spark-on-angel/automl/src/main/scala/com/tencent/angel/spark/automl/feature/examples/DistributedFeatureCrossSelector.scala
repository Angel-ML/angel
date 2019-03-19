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
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.operator.{VarianceSelector, VectorCartesian}
import org.apache.spark.sql.SparkSession

object DistributedFeatureCrossSelector {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val input = conf.get("spark.input.path")
    val numFeatures = conf.get("spark.num.feature")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val data = spark.read.format("libsvm")
      .option("numFeatures", numFeatures)
      .load(input)
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("f_f")

    val cartesianDF = cartesian.transform(data)
    println(s"cartesian data:")
    cartesianDF.show(1, truncate = false)

    val selector = new VarianceSelector()
      .setFeaturesCol("f_f")
      .setOutputCol("selected_f_f")
      .setNumTopFeatures(100)

    val selectorDF = selector.fit(cartesianDF).transform(cartesianDF)
    println(s"selector data:")
    selectorDF.show(1, truncate = false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "f_f"))
      .setOutputCol("assembled_features")

    val assemblerDF = assembler.transform(selectorDF)
    println(s"selector data:")
    assemblerDF.show(1, truncate = false)

    //    val pipeline = new Pipeline()
    //      .setStages(Array(cartesian, selector, assembler))
    //
    //    val crossDF = pipeline.fit(data).transform(data).persist()
    //    data.unpersist()
    //    crossDF.drop("f_f", "selected_f_f")
    //    crossDF.show(1)

    val splitDF = assemblerDF.randomSplit(Array(0.7, 0.3))

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

    spark.close()
  }
}
