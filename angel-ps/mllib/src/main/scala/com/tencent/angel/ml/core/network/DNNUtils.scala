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


package com.tencent.angel.ml.core.network

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.{AngelGraph, Layer}
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{DenseInputLayer, SparseInputLayer}
import com.tencent.angel.ml.core.network.layers.edge.losslayer.{SimpleLossLayer, SoftmaxLossLayer}
import com.tencent.angel.ml.core.network.layers.linear.FCLayer
import com.tencent.angel.ml.core.network.transfunc._
import com.tencent.angel.ml.core.optimizer.OptUtils
import com.tencent.angel.ml.core.optimizer.loss._

object DNNUtils {
  def getMLPInputLayer(conf: SharedConf)(implicit graph: AngelGraph): Layer = {
    val PAT = " *(\\d+), *(\\w+) *".r
    val inputParams = conf.get(MLConf.ML_MLP_INPUT_LAYER_PARAMS).trim

    (SharedConf.inputDataFormat, inputParams) match {
      case ("dense", PAT(outDim, transFunc)) if transFunc.toLowerCase == "identity" =>
        new DenseInputLayer("input", outDim.toInt, new Identity(), OptUtils.getOptimizer(MLConf.ML_DENSEINPUTLAYER_OPTIMIZER))
      case ("dense", PAT(outDim, transFunc)) if transFunc.toLowerCase == "tanh" =>
        new DenseInputLayer("input", outDim.toInt, new Tanh(), OptUtils.getOptimizer(MLConf.ML_DENSEINPUTLAYER_OPTIMIZER))
      case ("dense", PAT(outDim, transFunc)) if transFunc.toLowerCase == "sigmoid" =>
        new DenseInputLayer("input", outDim.toInt, new Sigmoid(), OptUtils.getOptimizer(MLConf.ML_DENSEINPUTLAYER_OPTIMIZER))
      case ("dense", PAT(outDim, transFunc)) if transFunc.toLowerCase == "relu" =>
        new DenseInputLayer("input", outDim.toInt, new Relu(), OptUtils.getOptimizer(MLConf.ML_DENSEINPUTLAYER_OPTIMIZER))
      case ("dense", PAT(outDim, transFunc)) if transFunc.toLowerCase == "softmax" =>
        new DenseInputLayer("input", outDim.toInt, new Softmax(), OptUtils.getOptimizer(MLConf.ML_DENSEINPUTLAYER_OPTIMIZER))
      case (_, PAT(outDim, transFunc)) if transFunc.toLowerCase == "identity" =>
        new SparseInputLayer("input", outDim.toInt, new Identity(), OptUtils.getOptimizer(MLConf.ML_SPARSEINPUTLAYER_OPTIMIZER))
      case (_, PAT(outDim, transFunc)) if transFunc.toLowerCase == "tanh" =>
        new SparseInputLayer("input", outDim.toInt, new Tanh(), OptUtils.getOptimizer(MLConf.ML_SPARSEINPUTLAYER_OPTIMIZER))
      case (_, PAT(outDim, transFunc)) if transFunc.toLowerCase == "sigmoid" =>
        new SparseInputLayer("input", outDim.toInt, new Sigmoid(), OptUtils.getOptimizer(MLConf.ML_SPARSEINPUTLAYER_OPTIMIZER))
      case (_, PAT(outDim, transFunc)) if transFunc.toLowerCase == "relu" =>
        new SparseInputLayer("input", outDim.toInt, new Relu(), OptUtils.getOptimizer(MLConf.ML_SPARSEINPUTLAYER_OPTIMIZER))
      case (_, PAT(outDim, transFunc)) if transFunc.toLowerCase == "softmax" =>
        new SparseInputLayer("input", outDim.toInt, new Softmax(), OptUtils.getOptimizer(MLConf.ML_SPARSEINPUTLAYER_OPTIMIZER))
      case _ => throw new AngelException("trans function is not supported!")
    }
  }

  def getMLPHiddenLayer(inputLayer: Layer, conf: SharedConf)(implicit graph: AngelGraph): Layer = {
    val PAT = "( *\\d+ *),( *\\w+ *)".r

    var mlpLayer: Layer = inputLayer
    conf.get(MLConf.ML_MLP_HIDEN_LAYER_PARAMS).trim.split("\\|").zipWithIndex.foreach {
      case (PAT(outDim, transFunc), idx) if transFunc.trim.toLowerCase == "relu" =>
        mlpLayer = new FCLayer(s"hidenLayer$idx", outDim.trim.toInt, mlpLayer, new Relu(), OptUtils.getOptimizer(MLConf.ML_FCLAYER_OPTIMIZER))
      case (PAT(outDim, transFunc), idx) if transFunc.trim.toLowerCase == "tanh" =>
        mlpLayer = new FCLayer(s"hidenLayer$idx", outDim.trim.toInt, mlpLayer, new Tanh(), OptUtils.getOptimizer(MLConf.ML_FCLAYER_OPTIMIZER))
      case (PAT(outDim, transFunc), idx) if transFunc.trim.toLowerCase == "sigmoid" =>
        mlpLayer = new FCLayer(s"hidenLayer$idx", outDim.trim.toInt, mlpLayer, new Sigmoid(), OptUtils.getOptimizer(MLConf.ML_FCLAYER_OPTIMIZER))
      case (PAT(outDim, transFunc), idx) if transFunc.trim.toLowerCase == "softmax" =>
        mlpLayer = new FCLayer(s"hidenLayer$idx", outDim.trim.toInt, mlpLayer, new Softmax(), OptUtils.getOptimizer(MLConf.ML_FCLAYER_OPTIMIZER))
      case (PAT(outDim, transFunc), idx) if transFunc.trim.toLowerCase == "identity" =>
        mlpLayer = new FCLayer(s"hidenLayer$idx", outDim.trim.toInt, mlpLayer, new Identity(), OptUtils.getOptimizer(MLConf.ML_FCLAYER_OPTIMIZER))
    }

    mlpLayer
  }

  def getMLPLossLayer(inputLayer: Layer, loss: LossFunc, conf: SharedConf)(implicit graph: AngelGraph): Layer = {
    val lossParams = conf.get(MLConf.ML_MLP_LOSS_LAYER_PARAMS).trim.toLowerCase

    lossParams match {
      case "softmaxloss" => new SoftmaxLossLayer("softmaxLossLayer", inputLayer, loss)
      case _ => new SimpleLossLayer("simpleLossLayer", inputLayer, loss)
    }
  }

  def getLossFunc(conf: SharedConf)(implicit graph: AngelGraph): LossFunc = {
    val lossParams = conf.get(MLConf.ML_MLP_LOSS_LAYER_PARAMS).trim.toLowerCase

    val PAT = "(\\w+ *):( *[0-9.]+)".r
    lossParams match {
      case "logloss" => new LogLoss()
      case "hingeloss" => new HingeLoss()
      case "l2loss" => new L2Loss()
      case "crossentropyloss" => new CrossEntropyLoss()
      case "softmaxloss" => new SoftmaxLoss()
      case PAT(lossName, delta) if lossName.trim == "huberloss" => new HuberLoss(delta.trim.toDouble)
      case _ => throw new AngelException("lossFunc is not supported!")
    }
  }
}
