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


package com.tencent.angel.ml.core.utils.paramsutils

object ParamKeys {
  val layer: String = "layer"
  val layers: String = "layers"
  val name: String = "name"
  val typeName: String = "type"
  val outputDim: String = "outputdim"
  val outputDims: String = "outputdims"
  val transFunc: String = "transfunc"
  val transFuncs: String = "transfuncs"
  val defaultTransFunc: String = "default_transfunc"
  val numFactors: String = "numfactors"
  val optimizer: String = "optimizer"
  val defaultOptimizer: String = "default_optimizer"
  val inputLayer: String = "inputlayer"
  val inputLayers: String = "inputlayers"
  val lossFunc: String = "lossfunc"
  val defaultLossFunc: String = "default_lossfunc"
  val dropout: String = "dropout"
  val reg1: String = "reg1"
  val reg2: String = "reg2"
  val lr: String = "lr"
  val alpha: String = "alpha"
  val beta: String = "beta"
  val gamma: String = "gamma"
  val delta: String = "delta"
  val momentum: String = "momentum"
  val proba: String = "proba"
  val actionType: String = "actiontype"
  val data: String = "data"
  val model: String = "model"
  val train: String = "train"
  val path: String = "path"
  val format: String = "format"
  val indexRange: String = "indexrange"
  val numField: String = "numfield"
  val validateRatio: String = "validateratio"
  val sampleRatio: String = "sampleratio"
  val useShuffle: String = "useshuffle"
  val posnegRatio: String = "posnegratio"
  val transLabel: String = "translabel"
  val loadPath: String = "loadpath"
  val savePath: String = "savepath"
  val modelType: String = "modeltype"
  val modelSize: String = "modelsize"
  val blockSize: String = "blockSize"
  val epoch: String = "epoch"
  val numUpdatePerEpoch: String = "numupdateperepoch"
  val batchSize: String = "batchsize"
  val decayClass: String = "decayclass"
  val decayAlpha: String = "decayalpha"
  val decayBeta: String = "decaybeta"
}
