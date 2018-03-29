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
 *
 */

package com.tencent.angel.ml.optimizer2.utils

import java.util

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TUpdate
import com.tencent.angel.ml.optimizer2.OptModel
import com.tencent.angel.worker.storage.DataBlock

object MeasureUtils {
  def calBinClassicationMetrics(measureData: DataBlock[LabeledData], thred: Double,
                                model: OptModel, params: util.HashMap[String, TUpdate]): (Double, Double, Double, Double, Double) = {
    val predict: Array[Double] = new Array[Double](measureData.size())
    val label: Array[Double] = new Array[Double](measureData.size())

    var (loss, minPred, maxPred) = (0.0, Double.MaxValue, Double.MinValue)
    (0 until measureData.size()).foreach { idx =>
      val entry = measureData.loopingRead()
      val (x, y) = (entry.getX, entry.getY)
      val predAndLoss = model.calPredAndLoss(x, y, params)
      val pred = predAndLoss._1
      if (pred > maxPred) maxPred = pred
      if (pred < minPred) minPred = pred
      predict(idx) = pred
      loss += predAndLoss._2
      label(idx) = y
    }

    var (acc, posTrue, totalTrue, negFalse, totalFalse) = (0.0, 0.0, 0.0, 0.0, 0.0)
    predict.zip(label).foreach { case (p, y) =>
      if (p >= thred && y >= 0) {
        acc += 1.0
        posTrue += 1.0
        totalTrue += 1.0
      } else if (p >= thred && y < 0) {
        totalFalse += 1.0
      } else if (p < thred && y < 0) {
        acc += 1.0
        negFalse += 1.0
        totalFalse += 1.0
      } else {
        // p < thred && y >= 0
        totalTrue += 1.0
      }
    }

    val auc = calAUC(predict, label, minPred, maxPred)

    (loss, acc / measureData.size(), auc, posTrue / totalTrue, negFalse / totalFalse)
  }

  private def calAUC(predict: Array[Double], label: Array[Double], minPred: Double, maxPred: Double): Double = {
    val step = 15
    val delta = (maxPred - minPred) / step
    val tuple = (-2 to step).map { idx =>
      if (idx == -2) {
        (0.0, 0.0)
      } else if (idx == -1) {
        (1.0, 1.0)
      } else {
        var (tp, tn, fp, fn) = (0, 0, 0, 0)
        val thred = minPred + idx * delta
        predict.zip(label).foreach { case (p, y) =>
          if (p >= thred && y >= 0) {
            tp += 1
          } else if (p >= thred && y < 0) {
            fp += 1
          } else if (p < thred && y < 0) {
            tn += 1
          } else {
            fn += 1
          }
        }
        (1.0 * fp / (fp + tn), 1.0 * tp / (tp + fn))
      }
    }.toList.sorted

    (0 until tuple.size - 1).map { idx =>
      val frist = tuple(idx)
      val second = tuple(idx + 1)
      0.5 * (second._1 - frist._1) * (frist._2 + second._2)
    }.sum
  }
}
