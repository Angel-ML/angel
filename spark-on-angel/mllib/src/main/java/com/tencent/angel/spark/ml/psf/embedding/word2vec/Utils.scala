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


package com.tencent.angel.spark.ml.psf.embedding.word2vec


object Utils {

  def getSentenceContext(sentence: Array[Int], inputIndex: Int, window: Int): Array[Int] = {
    assert(sentence.length > 1)
    val radius = window / 2
    val leftSize = if (inputIndex > radius) radius else inputIndex
    val rightSize = if (inputIndex + radius < sentence.length) radius else sentence.length - inputIndex - 1
    val outputs = Array.ofDim[Int](leftSize + rightSize)
    if (leftSize > 0)
      System.arraycopy(sentence, inputIndex - leftSize, outputs, 0, leftSize)
    if (rightSize > 0)
      System.arraycopy(sentence, inputIndex + 1, outputs, leftSize, rightSize)
    outputs
  }
}