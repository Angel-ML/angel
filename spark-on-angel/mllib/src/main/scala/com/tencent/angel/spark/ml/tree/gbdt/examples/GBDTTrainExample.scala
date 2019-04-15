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

package com.tencent.angel.spark.ml.tree.gbdt.examples

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.gbdt.trainer.GBDTTrainer
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.spark.{SparkConf, SparkContext}

object GBDTTrainExample {

  def main(args: Array[String]): Unit = {

    @transient val conf = new SparkConf().setMaster("local").setAppName("gbdt")

    val param = new GBDTParam

    // spark conf
    val numExecutor = 1
    val numCores = 1
    param.numWorker = numExecutor
    param.numThread = numCores
    conf.set("spark.task.cpus", numCores.toString)
    conf.set("spark.locality.wait", "0")
    conf.set("spark.memory.fraction", "0.7")
    conf.set("spark.memory.storageFraction", "0.8")
    conf.set("spark.task.maxFailures", "1")
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.network.timeout", "1000")
    conf.set("spark.executor.heartbeatInterval", "500")

    val params = ArgsUtil.parse(args)

    // dataset conf
    param.numClass = 3
    param.numFeature = 181

    // loss and metric
    param.lossFunc = "multi:logistic"
    param.evalMetrics = Array("error")
    param.multiStrategy = "multi-tree"
    if (param.isMultiClassMultiTree) param.lossFunc = "binary:logistic"
    param.multiGradCache = true

    // major algo conf
    param.featSampleRatio = 1.0f
    param.learningRate = 0.1f
    param.numSplit = 10
    param.numTree = 10
    if (param.isMultiClassMultiTree) param.numTree *= param.numClass
    param.maxDepth = 7
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = 4096 min maxNodeNum

    // less important algo conf
    param.histSubtraction = true
    param.lighterChildFirst = true
    param.fullHessian = false
    param.minChildWeight = 0.01f
    param.minNodeInstance = 10
    param.minSplitGain = 0.0f
    param.regAlpha = 0.0f
    param.regLambda = 1.0f
    param.maxLeafWeight = 0.0f

    println(s"Hyper-parameters:\n$param")

    val trainPath = "data/dna/dna.scale"
    val validPath = "data/dna/dna.scale.t"
    val modelPath = "xxx"

    @transient implicit val sc = new SparkContext(conf)

    try {
      val trainer = new GBDTTrainer(param)
      trainer.initialize(trainPath, validPath)
      val model = trainer.train()
      trainer.save(model, modelPath)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
    }
  }
}
