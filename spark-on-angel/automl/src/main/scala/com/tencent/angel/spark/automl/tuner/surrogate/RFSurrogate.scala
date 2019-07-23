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


package com.tencent.angel.spark.automl.tuner.surrogate

import com.tencent.angel.spark.automl.tuner.config.ConfigurationSpace
import org.apache.spark.ml.linalg.Vector
import com.tencent.angel.spark.automl.utils.DataUtils
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.{DataFrame, SparkSession}

class RFSurrogate(
                   override val cs: ConfigurationSpace,
                   override val minimize: Boolean = true)
  extends Surrogate(cs, minimize) {

  override val LOG: Log = LogFactory.getLog(classOf[RFSurrogate])

  var model: RandomForestRegressionModel = _
  val numTrees: Int = 5
  val maxDepth: Int = 2

  val ss = SparkSession.builder()
    .master("local")
    .appName("test")
    .getOrCreate()

  ss.sparkContext.setLogLevel("ERROR")

  override def train(): Unit = {

    if (preX.size < Math.pow(2, maxDepth - 1))
      return

    val data: DataFrame = DataUtils.parse(ss, schema, preX.toArray, preY.toArray)


    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)

    model = rf.fit(data)
  }

  /**
    * Predict means and variances for a single given X.
    *
    * @param X
    * @return a tuple of (mean, variance)
    */
  override def predict(X: Vector): (Double, Double) = {

    if (preX.size < Math.pow(2, maxDepth - 1)) {
      return (0.0, 0.0)
    }

    val preds = model.trees.map { tree: DecisionTreeRegressionModel =>
      val pred = tree.transform(DataUtils.parse(ss, schema, X))
      pred.select("prediction").first().getDouble(0)
    }

    //println(s"tree predictions of ${X.toArray.mkString(",")}: ${preds.mkString(",")}")

    val mean: Double = preds.sum / preds.length
    val variance = preds.map(x => Math.pow(x - mean, 2)).sum / preds.length

    //println(s"predict of ${X.toArray.mkString(",")}: mean[$mean] variance[$variance]")

    (mean, variance)
  }

  override def stop(): Unit = {
    ss.stop
  }

}