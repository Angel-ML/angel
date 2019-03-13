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


package com.tencent.angel.spark.automl.feature

import org.apache.spark.ml.{PipelineStage, Transformer}
import com.tencent.angel.spark.automl.feature.InToOutRelation.InToOutRelation

abstract class TransformerWrapper {

  val transformer: PipelineStage
  var parent: TransformerWrapper

  val relation: InToOutRelation

  val hasMultiInputs: Boolean
  val hasMultiOutputs: Boolean
  val needAncestorInputs: Boolean
  private val prefix = "out"

  val requiredInputCols: Array[String]
  val requiredOutputCols: Array[String]

  private var inputCols: Array[String] = _
  private var outputCols: Array[String] = _

  private var ancestorCols: Array[String] = _

  def getTransformer = transformer

  def setParent(parent: TransformerWrapper): Unit = this.parent = parent

  def setInputCols(cols: Array[String]): Unit = inputCols = cols

  def setOutputCols(cols: Array[String]): Unit = outputCols = cols

  def getInputCols: Array[String] = inputCols

  def getOutputCols: Array[String] = outputCols

  def setAncestorCols(cols: Array[String]): Unit = ancestorCols = cols

  def generateInputCols(): Unit = {
    //require(ancestorCols.contains(requiredInputCols), "Missing required input cols.")
    //    require(requiredInputCols.forall(ancestorCols.contains), "Missing required input cols.")
    // if transformer has required input cols, feed required input cols
    // if transformer needs all input cols, feed all input cols
    // if transformer has no required input cols, feed the output cols of the parent transformer
    if (ancestorCols.contains(requiredInputCols)) {
      setInputCols(requiredInputCols)
    } else if (needAncestorInputs) {
      setInputCols(ancestorCols)
    } else {
      setInputCols(parent.outputCols)
    }
  }

  def generateOutputCols(): Unit = {
    relation match {
      case InToOutRelation.Fixed =>
        setOutputCols(requiredOutputCols)
      case InToOutRelation.InPlace =>
        setOutputCols(inputCols)
      case InToOutRelation.OneToOne =>
        setOutputCols(Array(prefix + transformer.getClass.getSimpleName))
      case InToOutRelation.MultiToMulti =>
        setOutputCols(inputCols.map(prefix + _))
      case InToOutRelation.MultiToOne =>
        setOutputCols(Array(prefix + transformer.getClass.getName.toLowerCase))
      case _ =>
        throw new IncompatibleFiledExecption(
          "wrong relations between input and output of transformer")
    }
  }

  def declareInAndOut(): this.type
}
