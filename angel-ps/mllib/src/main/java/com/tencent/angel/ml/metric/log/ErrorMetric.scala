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

package com.tencent.angel.ml.metric.log

import com.tencent.angel.ml.metrics.Metric

class ErrorMetric(var sampleNum: Int) extends Metric{

  var globalErr: Double = 0.0

  override def merge(other: Metric): Metric = {
    this.sampleNum += other.asInstanceOf[ErrorMetric].sampleNum
    this.globalErr += (other.asInstanceOf[ErrorMetric].globalErr - this.globalErr) *
      other.asInstanceOf[ErrorMetric].sampleNum / (other.asInstanceOf[ErrorMetric].sampleNum + this.sampleNum)
    this
  }

  override def calculate: Double = {
    this.globalErr
  }

  override def setValues(values: Double*): Unit = {
    this.globalErr = values(0)
  }

  override def toString: String = {
    DEFAULT_METRIC_FORMAT.format(this.calculate)
  }
}

object ErrorMetric{
  def apply(sampleNum:Int) = {
    new ErrorMetric(sampleNum)
  }

}
