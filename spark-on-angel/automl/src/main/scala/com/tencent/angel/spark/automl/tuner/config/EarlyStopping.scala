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


package com.tencent.angel.spark.automl.tuner.config

/**
  * A single configuration
  *
  * @param patience : How long to wait after last time validation loss improved.
  *                 Default: 5
  * @param minimize   : Whether to minimize or maximize the val_score
  *                 Default: false
  */
class EarlyStopping(patience:Int=5,
                    var min_delta:Double = 0.0,
                    minimize:Boolean=false) {

  var counter: Int = 0
  var best_score: Double = Double.NegativeInfinity
  var early_stop: Boolean = false
  val pat = patience

  def greater(a: Double, b: Double): Boolean = {
    if (a > b) {
      return true
    }
    else {
      return false
    }
  }

  var monitor_op = greater _

  def less(a: Double, b: Double): Boolean = {
    if (a > b) {
      return false
    }
    else {
      return true
    }
  }

  if (minimize) {
    monitor_op = less _
    min_delta = -min_delta
    best_score = Double.PositiveInfinity
  }


  def update(val_score: Double): Unit = {
    val score = val_score
    if (monitor_op(score - min_delta, best_score)) {
      best_score = score
      counter = 0
    }
    else {
      counter += 1
      println(s"EarlyStopping counter: ${counter} out of ${patience}")
      if (counter >= patience) {
        early_stop = true
      }
    }
  }
}