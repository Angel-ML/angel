package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.EmbeddingConf
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.buildDataBatches
import com.tencent.angel.spark.ml.embedding.word2vec.Word2vecWorker
import com.tencent.angel.spark.ml.feature.Features
import org.apache.spark.{SparkConf, SparkContext}

object Word2vecWorkerExample {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)

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

    val input = "data/text8/text8.split.head"
    val output = "model/"

    val data = sc.textFile(input)
    data.cache()

    val corpus = Features.corpusStringToInt(sc.textFile(input))._1
    val docs = corpus.repartition(1)

    docs.cache()
    docs.count()

    data.unpersist()

    val numDocs = docs.count()
    val maxWordId = docs.map(_.max).max().toInt + 1
    val numTokens = docs.map(_.length).sum().toLong

    println(s"numDocs=$numDocs maxWordId=$maxWordId numTokens=$numTokens")
    val batchSize = params.getOrElse(EmbeddingConf.BATCHSIZE, "100").toInt

    println(s"Trainign SGNS with rooted pagerank")
    val sgns = new Word2vecWorker(maxWordId, params.updated(EmbeddingConf.MODELTYPE, "sgns"))
    val iterator = buildDataBatches(corpus, batchSize)
    sgns.train(iterator)

    println(s"Trainign CBOW")
    val cbow = new Word2vecWorker(maxWordId, params.updated(EmbeddingConf.MODELTYPE, "cbow"))
    cbow.train(iterator)

    PSContext.stop()
    sc.stop()
  }
}
