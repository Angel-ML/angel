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


package com.tencent.angel.spark.ml.tree.data

import org.apache.spark.ml.linalg.Vector
import com.tencent.angel.spark.ml.tree.exception.GBDTException

object Instance {

  def ensureLabel(labels: Array[Float], numLabel: Int): Boolean = {
    var needTrans: Boolean = false
    if (numLabel == 2) {
      val distinct = labels.distinct
      if (distinct.length < 2)
        throw new GBDTException("All labels equal to " + distinct.head.toInt)
      else if (distinct.length > 2)
        throw new GBDTException("More than 2 labels are provided: " + distinct.mkString(", "))
      else if (distinct.contains(-1f)) {
        println("[WARN] Change labels from -1 to 0")
        for (i <- labels.indices)
          if (labels(i) == -1f)
            labels(i) = 0f
        needTrans = true
      }
    } else {
      var min = Integer.MAX_VALUE
      var max = Integer.MIN_VALUE
      for (label <- labels) {
        val trueLabel = label.toInt
        min = Math.min(min, trueLabel)
        max = Math.max(max, trueLabel)
        if (label < 0 || label > numLabel)
          throw new GBDTException("Incorrect label: " + trueLabel)
      }
      if (max - min >= numLabel) {
        throw new GBDTException(s"Invalid range for labels: [$min, $max]")
      } else if (max == numLabel) {
        println(s"[WARN] Change range of labels from [1, $numLabel] to [0, ${numLabel - 1}]")
        for (i <- labels.indices) {
          labels(i) -= 1f
        }
        needTrans = true
      }
    }
    needTrans
  }

  def changeLabel(labels: Array[Float], numLabel: Int): Unit = {
    if (numLabel == 2) {
      println("[WARN] Change labels from -1 to 0")
      for (i <- labels.indices) {
        if (labels(i) == -1f)
          labels(i) = 0f
      }
    } else {
      println(s"[WARN] Change range of labels from [1, $numLabel] to [0, ${numLabel - 1}]")
      for (i <- labels.indices) {
        labels(i) -= 1f
      }
    }
  }

}

case class Instance(label: Double, feature: Vector) {
  override def toString: String = {
    s"($label, $feature)"
  }
}
