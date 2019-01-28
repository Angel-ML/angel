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

package com.tencent.angel.spark.ml.tree.objective.metric

import com.tencent.angel.spark.ml.tree.exception.GBDTException
import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric.Kind

import scala.util.Random

class AUCMetric extends EvalMetric {

  override def getKind: EvalMetric.Kind = Kind.AUC

  override def sum(preds: Array[Float], labels: Array[Float]): Double = {
    eval(preds, labels)
  }

  override def sum(preds: Array[Float], labels: Array[Float], start: Int, end: Int): Double = {
    eval(preds.slice(start, end), labels.slice(start, end))
  }

  override def avg(sum: Double, num: Int): Double = {
    sum / num
  }

  override def eval(preds: Array[Float], labels: Array[Float]): Double = {
    val scores: Array[(Float, Float)] = labels.zip(preds).sortBy(f => f._2)

    val numTotal = scores.length
    val numPositive = scores.count(f => f._1 > 0)
    val numNegative = numTotal - numPositive

    // calculate the summation of ranks for positive samples
    val sumRanks = scores.zipWithIndex.filter(f => f._1._1.toInt == 1)
      .map(f => (1.0 + f._2) / numPositive / numNegative).sum

    //val auc = (sumRanks - (numPositive * (numPositive + 1.0)) / 2.0) / (numPositive * numNegative)
    val auc = sumRanks.toFloat - (1.0 + numPositive) / numNegative / 2.0
    println(s"auc[$auc] numTotal[$numTotal] numPositive[$numPositive] numNegative[$numNegative] sumRanks[$sumRanks]")

    auc
  }

  override def evalOne(pred: Float, label: Float): Double = {
    throw new GBDTException("AUC does not support evalOne")
  }

  override def evalOne(pred: Array[Float], label: Float): Double = {
    throw new GBDTException("AUC does not support evalOne")
  }
}

object AUCMetric {
  def apply(): AUCMetric = new AUCMetric()

  def main(args: Array[String]): Unit = {
    val num = 10000000
    val labels: Array[Float] = Array.fill(num)(if (Random.nextDouble().toFloat > 0.5) 1 else 0)
    val preds: Array[Float] = labels.map { label =>
      if (Random.nextDouble() > 0.2) label else (1 - label)
    }
    val auc = AUCMetric().eval(preds, labels)
    println(s"auc[$auc]")
  }
}