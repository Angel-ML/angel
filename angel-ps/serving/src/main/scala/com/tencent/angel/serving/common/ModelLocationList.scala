/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving.common

import com.tencent.angel.serving.ServingLocation

/**
  * the model location list
  *
  * @param modelLocations the model locations
  */
case class ModelLocationList(modelLocations: Array[ModelLocation]) {

}

/**
  * the model location
  *
  * @param name           the model name
  * @param splitLocations the split locations
  */
case class ModelLocation(name: String, splitLocations: Array[ModelSplitLocation]) {
  override def toString: String = name + "=" + splitLocations.mkString("[", ",", "]")
}


/**
  * the model split location
  *
  * @param idx  the model split index
  * @param locs the model split locations
  */
case class ModelSplitLocation(idx: Int, locs: Array[ServingLocation]) {
  override def toString: String = idx + ":" + locs.mkString("(", "|", ")")
}
