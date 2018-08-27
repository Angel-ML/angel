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


package com.tencent.angel.ml.core.network.layers.linear

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.core.network.layers._
import org.apache.commons.logging.LogFactory


class BiOutterCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) {
  val LOG = LogFactory.getLog(classOf[BiOutterCross])
  val modelType: RowType = SharedConf.denseModelType

  @transient var output: Matrix = _
  @transient var gradOutput: Matrix = _

  override def calOutput(): Matrix = ???

  override def calGradOutput(): Matrix = ???
}
