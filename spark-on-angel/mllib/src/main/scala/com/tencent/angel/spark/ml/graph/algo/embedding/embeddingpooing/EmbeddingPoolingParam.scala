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

package com.tencent.angel.spark.ml.graph.algo.embedding.embeddingpooing

class EmbeddingPoolingParam extends Serializable {
  var embeddingDim: Int = _
  var batchSize: Int = _
  var maxIndex: Int = _
  var numPsPartitions: Int = 1
  var outputPath: String = _

  def setEmbeddingDim(embeddingDim: Int): this.type = {
    require(embeddingDim > 0, s"require embedding dimension > 0, $embeddingDim given")
    this.embeddingDim = embeddingDim
    this
  }

  def setBatchSize(batchSize: Int): this.type = {
    require(batchSize > 0, s"require batch size > 0, $batchSize given")
    this.batchSize = batchSize
    this
  }

  def setMaxIndex(maxIndex: Long): this.type = {
    require(maxIndex > 0 && maxIndex < Int.MaxValue, s"require maxIndex > 0 && maxIndex < Int.maxValue, $maxIndex given")
    this.maxIndex = maxIndex.toInt
    this
  }

  def setNumPSPart(numPSPart: Int): this.type = {
    require(numPSPart > 0, s"require num of PS part > 0, $numPSPart given")
    this.numPsPartitions = numPSPart
    this
  }

  def setOutputPath(outputPath: String): this.type = {
    this.outputPath = outputPath
    this
  }

}
