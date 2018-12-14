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


package com.tencent.angel.spark.ml.automl.feature

import org.apache.spark.ml.Transformer

import scala.collection.mutable.ArrayBuffer

abstract class TransformerWrapper {

  val transformer: Transformer
  var parentTransformer: Transformer

  val requiredInputCols: Array[String]
  val requiredOutputCols: Array[String]

  val inputCols: ArrayBuffer[String]
  val outputCols: ArrayBuffer[String]

  var parentCols: Array[String]

  def getTransformer: Transformer = transformer

  def setParent(parent: Transformer) = parentTransformer = parent

  def hasInputCol: Boolean

  def hasOutputCol: Boolean

  def getInputCols: Array[String] = inputCols.toArray

  def getOutputCols: Array[String] = outputCols.toArray

  def addInputCol(col: String): Unit = inputCols += col

  def addOutputCol(col: String): Unit = outputCols += col

  def setParentCols: Array[String] = parentCols

}
