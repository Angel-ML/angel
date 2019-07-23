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
import org.apache.spark.ml.feature.operator.{SelfCartesian, VectorFilterZero}
import org.apache.spark.sql.SparkSession

object FilterZeroExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val trainDF = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val maxDim = FeatureUtils.maxDim(trainDF)
    println(s"max dimension: $maxDim")

    // feature cross meta
    var crossInfo: Map[Int, FeatureCrossMeta] = Map[Int, FeatureCrossMeta]()
    (0 until maxDim).foreach(idx => crossInfo += idx -> FeatureCrossMeta(idx, idx.toString))

    val featureMap: Map[Int, Int] = Map[Int, Int]()

    val cartesian = new SelfCartesian()
      .setInputCol("features")
      .setOutputCol("cartesian_features")

    val filter = new VectorFilterZero(featureMap)
      .setInputCol("cartesian_features")
      .setOutputCol("filter_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, filter))

    val pipelineModel = pipeline.fit(trainDF)

    val filterDF = pipelineModel.transform(trainDF)

    println("nonzero features:")
    println(filter.featureMap.mkString(","))

    filterDF.show(1)

  }
}
