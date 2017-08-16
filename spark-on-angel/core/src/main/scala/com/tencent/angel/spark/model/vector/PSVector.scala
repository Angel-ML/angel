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

package com.tencent.angel.spark.model.vector

import com.tencent.angel.spark.model.PSModelProxy

/**
 * PSVector is a vector store on the PS nodes, and PSVectorProxy is the proxy of PSVector.
 * PSVector has three forms: LocalPSVector, RemotePSVector and BreezePSVector,
 * these three forms of PSVector have implement a set of operations for different situation.
 * LocalPSVector implements the operations for PSVector local form.
 * RemotePSVector implements the operations between PSVector and local data.
 * BreezePSVector implements the operations among PSVectors on PS nodes.
 */
abstract class PSVector extends Serializable {

  def proxy: PSModelProxy

  def length: Int = proxy.numDimensions

  def size: Int = length

}
