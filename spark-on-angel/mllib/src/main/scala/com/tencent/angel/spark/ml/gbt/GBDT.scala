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

import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.common.Instance
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}

class GBDT {
  val LOG = LogFactory.getLog(classOf[GBDTLearner])

  private val param: GBTreeParam = new GBTreeParam

  def setFeatureNum(num: Int): this.type = {
    param.featureNum = num
    this
  }

  def setSplitNum(num: Int): this.type = {
    param.splitNum = num
    this
  }

  def setMaxTreeNum(num: Int): this.type = {
    param.maxTreeNum = num
    this
  }

  def setPartitionNum(num: Int): this.type = {
    param.partitionNum = num
    this
  }

  def setTreeDepth(num: Int): this.type = {
    param.maxDepth = num
    this
  }

  def setFeatureSampleRate(rate: Double): this.type = {
    param.featureSampleRate = rate
    this
  }

  def setMinChildWeight(weight: Double): this.type = {
    param.minChildWeight = weight
    this
  }

  def setLearningRate(rate: Double): this.type = {
    param.learningRate = rate
    this
  }


  def train(trainset: RDD[Instance]): GBDTModel = {
    LOG.info("training GBDT model...")
    LOG.info(s"GBDT parameter:\n + ${param.toString}")

    val model = GBDT.train(trainset, param)

    LOG.info("training finished")
    model
  }

}

object GBDT {
  def train(trainSet: RDD[Instance], param: GBTreeParam): GBDTModel = {
    val learner = new GBDTLearner(param)
    learner.train(trainSet)
  }

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val splitNum = params.getOrElse("splitNum", "5").toInt
    val featSampleRate = params.getOrElse("featSampleRate", "1.0").toDouble
    val treeDepth = params.getOrElse("treeDepth", "6").toInt
    val maxTreeNum = params.getOrElse("maxTreeNum", "3").toInt
    val minChildWeight = params.getOrElse("minChildWeight", "2.0").toDouble
    val learningRate = params.getOrElse("learningRate", "0.3").toDouble

    val builder = SparkSession.builder()
      .master(mode)
      .appName(this.getClass.getSimpleName)

    if (mode.toLowerCase == "local") {
      builder.master("local")
        .config("spark.ps.mode", "LOCAL")
        .config("spark.ps.jars", "")
        .config("spark.ps.instances", "1")
        .config("spark.ps.cores", "1")
    }

    val spark = builder.getOrCreate()
    PSContext.getOrCreate(spark.sparkContext)

    val dataSet = DataLoader.loadLibsvm(input, partitionNum, sampleRate, -1)
      .map { case (label, feature) => Instance(label, feature)}
    val featureNum = dataSet.first().feature.size


    val gbdt = new GBDT()
      .setPartitionNum(partitionNum)
      .setSplitNum(splitNum)
      .setMaxTreeNum(maxTreeNum)
      .setTreeDepth(treeDepth)
      .setFeatureSampleRate(featSampleRate)
      .setFeatureNum(featureNum)
      .setMinChildWeight(minChildWeight)
      .setLearningRate(learningRate)

    val gbtModel = gbdt.train(dataSet)

    println(s"GBT model: \n + ${gbtModel.toString}")
  }
}
