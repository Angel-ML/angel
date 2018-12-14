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

package com.tencent.angel.spark.ml.automl.feature.preprocess

import com.tencent.angel.spark.ml.automl.feature.TransformerWrapper
import org.apache.spark.ml.Transformer

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.StopWordsRemover

class StopWordsRemoverWrapper extends TransformerWrapper {

  override val transformer: Transformer = new StopWordsRemover()
  override var parentTransformer: Transformer = _

  override val requiredInputCols: Array[String] = Array("words")
  override val requiredOutputCols: Array[String] = Array("filteredwords")

  override val inputCols: ArrayBuffer[String] = _
  override val outputCols: ArrayBuffer[String] = _

  override var parentCols: Array[String] = _

  override def hasInputCol: Boolean = true

  override def hasOutputCol: Boolean = true
}
