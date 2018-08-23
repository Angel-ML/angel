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


package com.tencent.angel.spark.ml.core.metric

import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector}
import org.apache.spark.rdd.RDD

class Precision extends Metric {

  override def cal(scores: RDD[(Matrix, Matrix)]): Double = {
    val pairs = scores.mapPartitions {
      case iter =>
        var numCorrect = 0L
        var numTotal = 0L
        iter.foreach { case (results, labels) =>
          val predict = results.getCol(2)
          val label = labels.getCol(0).asInstanceOf[IntFloatVector]
          val size = label.size()

          numTotal += size
          predict match {
            case double: IntDoubleVector =>
              for (i <- 0 until size) {
                if (double.get(i) > 0 && label.get(i) > 0) numCorrect += 1
                if (double.get(i) <= 0 && label.get(i) <= 0) numCorrect += 1
              }
            case float: IntFloatVector =>
              for (i <- 0 until size) {
                if (float.get(i) > 0 && label.get(i) > 0) numCorrect += 1
                if (float.get(i) <= 0 && label.get(i) <= 0) numCorrect += 1
              }
          }
        }
        Iterator.single((numCorrect, numTotal))
    }

    val (numCorrect, numTotal) = pairs.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
    return numCorrect * 1.0 / numTotal
  }

  override def toString: String = "Precision"
}
