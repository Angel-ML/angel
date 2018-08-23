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


package com.tencent.angel.spark.ml.embedding

object FastSigmoid {

  private final val MAX_SIGMOID = 8
  private final val SIGMOID_TABLE_SIZE = 512
  private final val LOG_TABLE_SIZE = 512
  private final val logTable = {
    Array.tabulate(LOG_TABLE_SIZE + 1)(i =>
      math.log((i + 1e-5) / LOG_TABLE_SIZE).toFloat
    )
  }
  private final val sigmoidTable = {
    Array.tabulate(SIGMOID_TABLE_SIZE + 1)(i => {
      val x = (i * 2 * MAX_SIGMOID).toDouble / SIGMOID_TABLE_SIZE - MAX_SIGMOID
      1.0f / (1.0f + math.exp(-x).toFloat)
    })
  }

  def sigmoid(x: Float): Float = {
    if (x < -MAX_SIGMOID)
      0.0f
    else if (x > MAX_SIGMOID)
      1.0f
    else
      sigmoidTable(((x + MAX_SIGMOID) * SIGMOID_TABLE_SIZE / MAX_SIGMOID / 2).toInt)
  }

  def log(x: Float): Float = {
    if (x > 1.0) 0.0f else logTable((x * LOG_TABLE_SIZE).toInt)
  }
}