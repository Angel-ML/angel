/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.ml.gbt

class GBTreeParam extends Serializable {
  // TODO: Parameter requirement.

  /** Tree parameter */
  var maxTreeNum: Int = 10

  var maxDepth: Int = 6

  /** Train data parameter */
  var classNum: Int = 2

  var featureNum: Int = 0

  var defaultValue: Double = 0.0

  var validateFraction: Double = 0.1

  // train set RDD partition number
  var partitionNum: Int = 100

  var splitNum: Int = 10

  var featureSampleRate: Double = 1.0

  /** Learning Parameter */
  var learningRate: Double = 0.3

  // L2 regularization parameter
  var regLambda: Double = 1.0

  // L1 regularization parameter
  var regAlpha: Double = 0

  var loss: Loss = new LeastSquareLoss

  var minSplitLoss: Double = 0.0

  var minChildWeight: Double = 1.0

  var maxDeltaStep: Double = 0.0

  // accuracy of sketch
  var sketchEps = 0.03

  // leaf vector size
  var leafVectorSize = 0

  // TODO:
  override def clone(): GBTreeParam = ???


  def sampledFeatNum: Int = (featureNum * featureSampleRate).toInt

  def maxNodeNum: Int = (math.pow(2, maxDepth) - 1).toInt

  override def toString: String = {
    s"featureNum: $featureNum\n" +
      s"classNum: $classNum\n" +
      s"maxTreeNum: $maxTreeNum \n" +
      s"maxDepth: $maxDepth \n" +
      s"partitionNum: $partitionNum\n" +
      s"splitNum: $splitNum\n" +
      s"featureSampleRate: $featureSampleRate\n" +
      s"learningRate: $learningRate"
  }

}

object GBTreeParam {
  // TODO: 
  def apply(): GBTreeParam = ???
}

