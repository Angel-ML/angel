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

    PSContext.getOrCreate(sc)

    val input = params.getOrElse("input", "")
    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val windowSize = params.getOrElse("window", "10").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val numPartitions = params.getOrElse("numParts", "10").toInt

    val numExecutors = SparkUtils.getNumExecutors(conf)

    val (corpus, _) = Features.corpusStringToInt(sc.textFile(input))
    val docs = SubSampling.sampling(corpus).repartition(numExecutors)
    docs.cache()

    val numDocs = docs.count()
    val maxWordId = docs.map(_.max).max().toLong + 1
    val numTokens = docs.map(_.length).sum().toLong
    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")

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

    new Word2VecModel(param).train(docs, param, None)
  }

}
