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

package com.tencent.angel.ml.metric

import com.tencent.angel.conf.AngelConf

/**
  * Algorithm metric calculate interface
  */


abstract class Metric {


  /**
    * Set metric calculate dependency counters
    */
  def setValue(value:Double)

  /**
    * Merge dependency counters
 *
    * @param other
    * @return
    */
  def merge(other:this.type) : this.type

  /**
    * Use counters calculate metric
 *
    * @return metric value
    */
  def calculate() : Double

  override def toString(): String = {
   AngelConf.DEFAULT_METRIC_FORMAT.format(this.calculate)
  }
}
