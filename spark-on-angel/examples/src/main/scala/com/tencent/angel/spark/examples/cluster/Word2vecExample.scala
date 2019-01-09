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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.Param
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel
import com.tencent.angel.spark.ml.feature.{Features, SubSampling}
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Word2vecExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val conf = new SparkConf()
    val sc   = new SparkContext(conf)

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    conf.set(AngelConf.ANGEL_PS_BACKUP_MATRICES, "")

    PSContext.getOrCreate(sc)

    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val windowSize = params.getOrElse("window", "10").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numPartitions = params.getOrElse("numParts", "10").toInt
    val withSubSample = params.getOrElse("subSample", "true").toBoolean
    val withRemapping = params.getOrElse("remapping", "true").toBoolean
    val modelType = params.getOrElse("modelType", "cbow")
    val checkpointInterval = params.getOrElse("interval", "10").toInt

    val numCores = SparkUtils.getNumCores(conf)

    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    val numDataPartitions = (numCores * 6.25).toInt

    val data = sc.textFile(input)
    data.persist(StorageLevel.DISK_ONLY)

    var corpus: RDD[Array[Int]] = null
    var denseToString: Option[RDD[(Int, String)]] = None

    if (withRemapping) {
      val temp = Features.corpusStringToInt(data)
      corpus = temp._1
      denseToString = Some(temp._2)
    } else {
      corpus = Features.corpusStringToIntWithoutRemapping(data)
    }

    val (maxWordId, docs) = if (withSubSample) {
      corpus.persist(StorageLevel.DISK_ONLY)
      val subsampleTmp = SubSampling.sampling(corpus)
      (subsampleTmp._1, subsampleTmp._2.repartition(numDataPartitions))
    } else {
      val tmp = corpus.repartition(numDataPartitions)
      (tmp.map(_.max).max().toLong + 1, tmp)
    }
    docs.persist(StorageLevel.DISK_ONLY)

    val numDocs = docs.count()
    val numTokens = docs.map(_.length).sum().toLong
    val maxLength = docs.map(_.length).max()
    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens maxLength=$maxLength")

    corpus.unpersist()
    data.unpersist()

    val param = new Param()
      .setLearningRate(stepSize)
      .setEmbeddingDim(embeddingDim)
      .setWindowSize(windowSize)
      .setBatchSize(batchSize)
      .setSeed(Random.nextInt())
      .setNumPSPart(Some(numPartitions))
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setMaxIndex(maxWordId)
      .setNumRowDataSet(numDocs)
      .setMaxLength(maxLength)
      .setModel(modelType)
      .setModelCPInterval(checkpointInterval)

    val model = new Word2VecModel(param)
    model.train(docs, param, output + "/embedding")
    model.save(output + "/embedding", numEpoch)
    denseToString.map(rdd => rdd.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping"))

    PSContext.stop()
    sc.stop()
  }

}