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
package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.buildDataBatches
import com.tencent.angel.spark.ml.embedding.word2vec.Word2vecWorker
import com.tencent.angel.spark.ml.feature.Features
import org.apache.spark.{SparkConf, SparkContext}

object Word2vecWorkerExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("Word2vec example")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceMap].getName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)

    val input = "data/text8/text8.split.remapping"
    val output = "model/"

    val data = sc.textFile(input)
    data.cache()

    val (corpus) = Features.corpusStringToIntWithoutRemapping(sc.textFile(input))
    val docs = corpus.repartition(1)

    docs.cache()
    docs.count()

    data.unpersist()

    val numDocs = docs.count()
    val maxWordId = docs.map(_.max).max().toLong + 1
    val numTokens = docs.map(_.length).sum().toLong

    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")

    val numNodePerRow = 1000
    val modelType = "cbow"
    val numPart = 1
    val dimension = 100
    val batchSize = 100

    val learnRate = 0.1f
    val window = 5
    val negative = 5

    println(s"batchSize=$batchSize learnRate=$learnRate window=$window negative=$negative")
    val model = new Word2vecWorker(maxWordId.toInt, dimension, "cbow", numPart, numNodePerRow, 2017)
    val iterator = buildDataBatches(corpus, batchSize)
    model.train(iterator, negative, 5, learnRate, window, "")

    PSContext.stop()
    sc.stop()
  }
}
