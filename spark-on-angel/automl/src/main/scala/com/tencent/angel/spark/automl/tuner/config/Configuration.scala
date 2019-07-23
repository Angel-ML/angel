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

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._

/**
  * A single configuration
  *
  * @param configSpace : The configuration space for this configuration
  * @param vector      : A vector for efficient representation of configuration.
  */
class Configuration(
                     param2Idx: Map[String, Int],
                     param2Doc: Map[String, String],
                     vector: Vector) {

  def getVector: Vector = vector

  def getParamMap: ParamMap = {
    val paramMap = ParamMap.empty
    for (name: String <- param2Idx.keys) {
      val param: Param[Double] = new Param(this.toString, name, param2Doc.getOrElse(name, ""))
      paramMap.put(param, vector(param2Idx(name)))
    }
    paramMap
  }

  def getValues: Array[Double] = vector.toArray

  def keys: List[String] = param2Idx.keys.toList

  def get(name: String): Double = get(param2Idx.getOrElse(name, -1))

  def get(idx: Int): Double = vector(idx)

  def contains(name: String): Boolean = param2Idx.contains(name)
}
