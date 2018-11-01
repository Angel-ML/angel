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

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.SparkUtils
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.Param
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel
import com.tencent.angel.spark.ml.feature.{Features, SubSampling}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Word2vecExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    val conf = new SparkConf()
    val sc   = new SparkContext(conf)

//    PSContext.getOrCreate(sc)

    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val windowSize = params.getOrElse("window", "10").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numPartitions = params.getOrElse("numParts", "10").toInt

    val numExecutors = SparkUtils.getNumExecutors(conf)

    val (corpus, denseToString) = Features.corpusStringToInt2(sc.textFile(input))


    val docs = corpus.repartition(numExecutors)
    docs.cache()

    val numDocs = docs.count()
    val maxWordId = docs.map(_.max).max().toLong + 1
    val numTokens = docs.map(_.length).sum().toLong
    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")

//    corpus.map(f => f.mkString(" ")).saveAsTextFile(output + "/corpus_ints")
//    denseToString.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping")


    val param = new Param()
      .setLearningRate(stepSize)
      .setEmbeddingDim(embeddingDim)
      .setWindowSize(windowSize)
      .setBatchSize(batchSize)
      .setNodesNumPerRow(Some(100))
      .setSeed(Random.nextInt())
      .setNumPSPart(Some(numPartitions))
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setMaxIndex(maxWordId)
      .setNumRowDataSet(numDocs)

    val model = new Word2VecModel(param)
    model.train(docs, param)
    model.save(output + "/embedding", 0)
    denseToString.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping")

    PSContext.stop()
    sc.stop()
  }

}
