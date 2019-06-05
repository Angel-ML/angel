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


package com.tencent.angel.ml.core.network.layers.verge


import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys, MLException}
import com.tencent.angel.ml.core.variable.{EmbedUtils, EmbedVariable, Variable}
import com.tencent.angel.ml.math2.utils.{MatrixUtils, VectorUtils}
import com.tencent.angel.ml.math2.matrix._
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

class Embedding(name: String, outputDim: Int, val numFactors: Int, override val optimizer: Optimizer)(implicit graph: Graph)
  extends InputLayer(name, outputDim) with Trainable {
  graph.addTrainableLayer(this)
  private val LOG = LogFactory.getLog(classOf[Embedding])

  private val formatClassName = SharedConf.get().getString(
    MLCoreConf.ML_EMBEDDING_MATRIX_OUTPUT_FORMAT,
    MLCoreConf.DEFAULT_ML_EMBEDDING_MATRIX_OUTPUT_FORMAT)
  private val embedding: EmbedVariable = graph.provider.getEmbedVariable(s"${name}_embedding",
    SharedConf.indexRange.toInt, numFactors, optimizer, formatClassName, graph.taskNum)

  override protected def doForward(input: Matrix): Matrix = {
    embedding.snapshot()
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Unit = {
    val gradInputData = gradInput match {
      case mat: BlasDoubleMatrix => MatrixUtils.blas2RBCompDense(mat.asInstanceOf[BlasDoubleMatrix], numFactors)
      case mat: BlasFloatMatrix => MatrixUtils.blas2RBCompDense(mat.asInstanceOf[BlasFloatMatrix], numFactors)
      case mat: RBCompIntDoubleMatrix => mat
      case mat: RBCompIntFloatMatrix => mat
      case _ => throw MLException("Error in embedding gradInput, not an instance of CompMatrix.")
    }
    val gradValue = EmbedUtils.calGradient(input, gradInputData)
    variableManager.putSlot(embedding.asInstanceOf[Variable], gradValue)
  }

  override def toString: String = {
    s"Embedding name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.numFactorsKey -> numFactors) ~
      (LayerKeys.optimizerKey -> optimizer.toJson)

    JField(name, layerJson)
  }

}
