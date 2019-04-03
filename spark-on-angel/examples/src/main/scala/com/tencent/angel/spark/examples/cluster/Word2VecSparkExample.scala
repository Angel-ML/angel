package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.embedding.EmbeddingConf
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.buildDataBatches
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecSpark
import com.tencent.angel.spark.ml.feature.Features
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.RangePartitioner
//import org.apache.spark.rdd.PipedRDD
//import org.apache.spark.SparkFiles
//import org.apache.spark.rdd.CoGroupedRDD
//import com.tencent.angel.exception


object Word2VecSparkExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("word2vec-spark")
    val sc = new SparkContext(sparkConf)

    val params = ArgsUtil.parse(args)
    val input = params.getOrElse(EmbeddingConf.INPUTPATH, "")

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

    val model = new Word2VecSpark(maxWordId.toInt, params)
    val iterator = buildDataBatches(docs, params.getOrElse(EmbeddingConf.BATCHSIZE, "1000").toInt)
    model.train(iterator)
    model.saveModel(index2word)

    SparkContext.getOrCreate().stop()

  }

}
