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


package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.utils.{DataParser, NetUtils}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.SparkUtils
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.spark.{SparkConf, SparkContext}

object OfflineRunner {


  def parse(text: String, dim: Int): LabeledData = {
    if (null == text) {
      return null
    }

    var splits = text.trim.split(" ")

    if (splits.length < 1)
      return null

    var y = splits(0).toDouble
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length

    val keys: Array[Int] = new Array[Int](len)
    val vals: Array[Float] = new Array[Float](len)

    // y should be +1 or -1 when classification.
    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      val kv = value.trim.split(":")
      keys(indx2) = kv(0).toInt
      vals(indx2) = kv(1).toFloat
    }
    val x = VFactory.sparseFloatVector(dim, keys, vals)
    new LabeledData(x, y)
  }

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val actionType = params.getOrElse("actionType", "train")
    val network = params.getOrElse("network", "LogisticRegression")
    val modelPath = params.getOrElse("model", "")

    // set running mode, use angel_ps mode for spark
    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

    // build SharedConf with params
    SharedConf.addMap(params)

    val dim = SharedConf.indexRange.toInt

    println(s"dim=$dim")

    // load data
    val conf = new SparkConf()
    val sc   = new SparkContext(conf)
    val data = sc.textFile(input)
      .repartition(SparkUtils.getNumExecutors(conf))
      .map(f => parse(f, dim))

    // calculating the feature index
//    data.cache()
//    val dim = data.map(s => s.getX.getStorage.asInstanceOf[IntFloatSparseVectorStorage].getIndices.max).max() + 1
//    data.foreach(f => f.getX.asInstanceOf[IntFloatVector].setDim(dim))

//    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, dim)
//    println(s"dim=$dim")

    // start PS
    PSContext.getOrCreate(sc)

    val className = "com.tencent.angel.spark.ml.classification." + network
    val model = GraphModel(className)
    val learner = new OfflineLearner

    // init model here
    model.init(data.getNumPartitions)

    actionType match {
      case MLConf.ANGEL_ML_TRAIN =>
        // get model path, load it first
        if (modelPath.length > 0) model.load(modelPath)
        // train
        learner.train(data, model)
        // save model
        if (output.length > 0) model.save(output)

      case MLConf.ANGEL_ML_PREDICT =>
        if (modelPath.length == 0)
          throw new AngelException("Should set model path for predict!")

        model.load(modelPath)
        val predict = learner.predict(data, model)
        if (output.length > 0)
          predict.map(f => s"${f._1} ${f._2}").saveAsTextFile(output)
    }
  }
}
