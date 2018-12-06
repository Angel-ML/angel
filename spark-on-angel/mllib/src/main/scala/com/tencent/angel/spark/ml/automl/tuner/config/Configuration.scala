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


package com.tencent.angel.spark.ml.automl.tuner.config

import com.tencent.angel.ml.math2.vector.IntFloatVector

/**
  * A single configuration
  *
  * @param configSpace : The configuration space for this configuration
  * @param vector      : A vector for efficient representation of configuration.
  */
class Configuration(configSpace: ConfigurationSpace, vector: IntFloatVector) {

  def getVector: IntFloatVector = vector

  def getValues: List[Float] = vector.getStorage.getValues.toList

  def keys: List[String] = configSpace.param2Idx.keys.toList

  def get(name: String): Float = get(configSpace.param2Idx.getOrElse(name, -1))

  def get(idx: Int): Float = vector.get(idx)

  def contains(name: String): Boolean = configSpace.param2Idx.contains(name)
}
