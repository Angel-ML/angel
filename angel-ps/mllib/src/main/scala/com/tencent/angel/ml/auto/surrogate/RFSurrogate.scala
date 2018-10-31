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


package com.tencent.angel.ml.auto.surrogate
import com.tencent.angel.ml.auto.utils.DataUtils
import com.tencent.angel.ml.math2.vector.IntFloatVector
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.{SparkConf, SparkContext}

class RFSurrogate(override val numParams: Int, override val minimize: Boolean = true)
  extends Surrogate(numParams, minimize) {
  override val LOG: Log = LogFactory.getLog(classOf[RFSurrogate])

  var model: GradientBoostedTreesModel = _
  val numIter: Int = 5
  val maxDepth: Int = 2
  val conf = new SparkConf().setMaster("local").setAppName("RandomForest")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  override def train(): Unit = {

    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = numIter
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    println(s"surrogate has ${curX.size} evaluations")

    if (curX.size < Math.pow(2, maxDepth - 1))
      return

    //curX.zip(curY).foreach( tuple => print(tuple._1, tuple._2))

    val data = DataUtils.parse(sc, curX.toList, curY.toList)

    model = GradientBoostedTrees.train(data, boostingStrategy)

  }

  /**
    * Predict means and variances for a single given X.
    *
    * @param X
    * @return a tuple of (mean, variance)
    */
  override def predict(X: IntFloatVector): (Float, Float) = {

    if (curX.size < Math.pow(2, maxDepth - 1)) {
      return (0.0f, 0.0f)
    }

    val preds: Array[Double] = model.trees.map(_.predict(DataUtils.parse(X)))

    println(s"predict of ${X.getStorage.getValues.mkString(",")}: ${preds.mkString(",")}")

    val mean: Double = preds.sum / preds.length
    val variance = preds.map(x => Math.pow(x - mean, 2)).sum / preds.length

    println(s"prediction mean $mean variance $variance")

    (mean.toFloat, variance.toFloat)
  }

  override def stop(): Unit = {
    sc.stop
  }

}
