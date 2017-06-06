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
 *
 */

package com.tencent.angel.spark.ml.common

/**
 * Learner is a trait for all supervised machine learning algorithm.
 */
trait Learner {

  /**
   *
   * @param input train data path, HDFS or local path.
   * @param validateSet validate data, if validateSet == null, it means train without validate data
   * @return trained Model
   */
  def train(input: String, validateSet: String): Model

  /**
   *
   * @param input predict data path, HDFS or local path.
   * @param output the path for predict result to save
   * @param model trained Model
   */
  def predict(input: String, output: String, model: Model)


  /**
   *
   * @param modelPath model path.
   * @return
   */
  def loadModel(modelPath: String): Model

  def process(
      actionType: String,
      input: String,
      modelPath: String,
      validateSet: String,
      output: String) {
    actionType match {
      case "train" =>
        train(input, validateSet).save(modelPath)

      case "predict" =>
        val model = loadModel(modelPath)
        predict(input, output, model)
    }
  }
}
