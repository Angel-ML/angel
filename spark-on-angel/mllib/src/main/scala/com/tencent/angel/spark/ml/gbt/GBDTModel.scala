/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.ml.gbt

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.tencent.angel.spark.ml.common.Instance
import com.tencent.angel.spark.models.MLModel
import com.tencent.angel.spark.models.matrix.{DensePSMatrix, PSMatrix}

class GBDTModel(param: GBTreeParam) extends MLModel {
  private var treeNum = 0
  private val splitFeatureMat = DensePSMatrix.fill(param.maxTreeNum, param.maxNodeNum, -1.0)
  private val splitValueMat = PSMatrix.dense(param.maxTreeNum, param.maxNodeNum)
  private val leafWeightMat = PSMatrix.dense(param.maxTreeNum, param.maxNodeNum)

  def updateSplitFeature(treeId: Int, featIds: Array[Int]): Unit = {
    if (treeId + 1 > treeNum) treeNum = treeId + 1

    val psFeatureIds = splitFeatureMat.pull(treeId)

    featIds.indices.foreach { index =>
      if (featIds(index) != -1) {
        psFeatureIds(index) = featIds(index)
      }
    }

    splitFeatureMat.push(treeId, psFeatureIds)
  }

  def updateSplitValue(treeId: Int, values: Array[Double]): Unit = {
    splitValueMat.increment(treeId, values)
  }

  def updateLeafWeight(treeId: Int, weights: Array[Double]): Unit = {
    leafWeightMat.push(treeId, weights)
  }

  def getSplitFeature(treeId: Int): Array[Int] = {
    splitFeatureMat.pull(treeId).map(_.toInt)
  }

  def getSplitValue(treeId: Int): Array[Double] = {
    splitValueMat.pull(treeId)
  }

  def getLeafWeight(treeId: Int): Array[Double] = {
    leafWeightMat.pull(treeId)
  }



  override def predict(dataset: DataFrame): DataFrame = ???

  def predict(feature: Vector,
      splitFeats: Array[Array[Int]],
      splitValues: Array[Array[Double]],
      leafWeights: Array[Array[Double]],
      loss: Loss): Double = {

    val featArray = feature.toArray

    val preds = (0 until treeNum).map { treeId =>
      val splitFeat = splitFeats(treeId)
      val splitValue = splitValues(treeId)

      var currNode = 0
      while (currNode < param.maxNodeNum && splitFeat(currNode) != -1) {
        val fId = splitFeat(currNode)
        val fValue = splitValue(currNode)
        if (featArray(fId) <= fValue) {
          currNode = currNode * 2 + 1
        } else {
          currNode = currNode * 2 + 2
        }
      }
      leafWeights(treeId)(currNode)
    }

    var prediction = 0.0
    preds.foreach { p =>
      prediction += p * param.learningRate
    }
    loss.transPred(prediction.toFloat).toDouble
  }

  def predict(dataset: RDD[Vector], loss: Loss): RDD[Double] = {
    dataset.mapPartitions { iter =>
      val splitFeatures = (0 until treeNum).toArray
        .map { treeId => splitFeatureMat.pull(treeId).map(_.toInt) }
      val splitValues = (0 until treeNum).toArray
        .map { treeId => splitValueMat.pull(treeId) }
      val leafWeight = (0 until treeNum).toArray
        .map { treeId => leafWeightMat.pull(treeId) }

      iter.map { feature =>
        predict(feature, splitFeatures, splitValues, leafWeight, loss)
      }
    }
  }


  def evaluate(dataset: RDD[Instance]): Double = {
    if (dataset.count() == 0) return Double.NaN
    val prediction = predict(dataset.map(_.feature), this.param.loss)
    val scoreAndLabel = dataset.zip(prediction).map { case (instance, score) =>
      Tuple2(score, instance.label)
    }

    param.loss.evalMetric match {
      // RMSE for LeastSquareLoss
      case "RMSE" =>
        val metric = new RegressionMetrics(scoreAndLabel)
        metric.rootMeanSquaredError
      // AUC for LogisticLoss
      case "AUC" =>
        val metric = new BinaryClassificationMetrics(scoreAndLabel)
        metric.areaUnderROC
    }
  }


  override def save(modelPath: String): Unit = ???

  override def toString: String = {

    val featStr = (0 until treeNum).toArray
      .map { treeId =>
        splitFeatureMat.pull(treeId).map(_.toInt).mkString(s"treeId $treeId: ", " ", "")
      }.mkString("\n")
    val valueStr = (0 until treeNum).toArray
      .map { treeId => splitValueMat.pull(treeId).mkString(s"treeId $treeId: ", " ", "") }.mkString("\n")
    val weightStr = (0 until treeNum).toArray
      .map { treeId => leafWeightMat.pull(treeId).mkString(s"treeId $treeId: ", " ", "") }.mkString("\n")

    s"\nsplit feature: \n$featStr" +
    s"\nsplit value: \n$valueStr" +
    s"\nleaf weight: \n$weightStr"
  }
}

object GBDTModel {
  def loadModel(modelPath: String): GBDTModel = ???
}
