package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.EmbeddingConf
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.buildDataBatches
import com.tencent.angel.spark.ml.embedding.word2vec.Word2vecWorker
import com.tencent.angel.spark.ml.feature.Features
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

object Word2vecWorkerExample {

  def start(): Unit = {
    val conf = new SparkConf()

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceMap].getName)

    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {

    start()
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse(EmbeddingConf.INPUTPATH, "")

    val sc = SparkContext.getOrCreate()
    val data = sc.textFile(input)
    data.cache()

    val (corpus, index2word) = Features.corpusStringToInt(sc.textFile(input))

    val numCores = SparkUtils.getNumCores(sc.getConf)

    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    val numDataPartitions = (numCores * 6.25).toInt
    val docs = corpus.repartition(numDataPartitions)

    docs.cache()
    docs.count()

    data.unpersist()

    val numDocs = docs.count()
    val maxWordId = docs.map(_.max).max().toLong + 1
    val numTokens = docs.map(_.length).sum().toLong

    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")

    val model = new Word2vecWorker(maxWordId.toInt, params)
    val iterator = buildDataBatches(docs, params.getOrElse(EmbeddingConf.BATCHSIZE, "1000").toInt)
    model.train(iterator)
    model.saveModel(index2word)

    stop()
  }

}
