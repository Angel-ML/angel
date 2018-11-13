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


package com.tencent.angel.spark.ml.embedding

class Param extends Serializable {

  var partitionNum: Int = _
  var windowSize: Int = _
  var embeddingDim: Int = _
  var negSample: Int = _
  var learningRate: Float = _
  var batchSize: Int = _
  var numEpoch: Int = _
  var maxIndex: Int = _
  var sampleRate: Float = _
  var numPSPart: Int = 1
  var modelPath: String = _
  var checkpointInterval: Int = Int.MaxValue
  var order: Int = _
  var nodesNumPerRow: Int = -1
  var numRowDataSet: Option[Long] = None
  var seed: Int = _
  var maxLength: Int = -1
  var model: String = "cbow"

  def setModel(model: String): this.type = {
    this.model = model
    this
  }

  def setMaxLength(maxLength: Int): this.type = {
    this.maxLength = maxLength
    this
  }

  def setNumRowDataSet(numRowDataSet: Long): this.type = {
    this.numRowDataSet = Some(numRowDataSet)
    this
  }

  def setSeed(seed: Int): this.type = {
    this.seed = seed
    this
  }

  def setPartitionNum(partitionNum: Int): this.type = {
    require(partitionNum > 0, s"require partitionNum > 0, $partitionNum given")
    this.partitionNum = partitionNum
    this
  }

  def setWindowSize(windowSize: Int): this.type = {
    require(windowSize > 0, s"require windowSize > 0, $windowSize given")
    this.windowSize = windowSize
    this
  }

  def setEmbeddingDim(embeddingDim: Int): this.type = {
    require(embeddingDim > 0, s"require embedding dimension > 0, $embeddingDim given")
    this.embeddingDim = embeddingDim
    this
  }

  def setNegSample(negSample: Int): this.type = {
    require(negSample > 0, s"require num of negative sample > 0, $negSample given")
    this.negSample = negSample
    this
  }

  def setLearningRate(learningRate: Float): this.type = {
    require(learningRate > 0, s"require learning rate > 0, $learningRate given")
    this.learningRate = learningRate
    this
  }

  def setBatchSize(batchSize: Int): this.type = {
    require(batchSize > 0, s"require batch size > 0, $batchSize given")
    this.batchSize = batchSize
    this
  }

  def setNumEpoch(numEpoch: Int): this.type = {
    require(numEpoch > 0, s"require num of epoch > 0, $numEpoch given")
    this.numEpoch = numEpoch
    this
  }

  def setMaxIndex(maxIndex: Long): this.type = {
    require(maxIndex > 0 && maxIndex < Int.MaxValue, s"require maxIndex > 0 && maxIndex < Int.maxValue, $maxIndex given")
    this.maxIndex = maxIndex.toInt
    this
  }

  def setSampleRate(sampleRate: Float): this.type = {
    require(sampleRate > 0, s"sample rate belongs to [0, 1], $sampleRate given")
    this.sampleRate = sampleRate
    this
  }

  def setModelPath(modelPath: String): this.type = {
    require(null != modelPath && modelPath.nonEmpty, s"require non empty path to save model, $modelPath given")
    this.modelPath = modelPath
    this
  }

  def setModelCPInterval(modelCPInterval: Int): this.type = {
    require(modelCPInterval > 0, s"model checkpoint interval > 0, $modelCPInterval given")
    this.checkpointInterval = modelCPInterval
    this
  }

  def setOrder(order: Int): this.type = {
    require(order == 1 || order == 2, s"order equals 1 or 2, $order given")
    this.order = order
    this
  }

  def setNumPSPart(numPSPart: Option[Int]): this.type = {
    require(numPSPart.fold(true)(_ > 0), s"require num of PS part > 0, $numPSPart given")
    numPSPart.foreach(this.numPSPart = _)
    this
  }

  def setNodesNumPerRow(nodesNumPerRow: Option[Int]): this.type = {
    nodesNumPerRow.foreach(this.nodesNumPerRow = _)
    this
  }


}
