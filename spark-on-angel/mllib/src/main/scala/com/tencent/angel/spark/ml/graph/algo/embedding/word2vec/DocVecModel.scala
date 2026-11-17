package com.tencent.angel.spark.ml.graph.algo.embedding.word2vec

import java.util.concurrent.TimeUnit

import com.tencent.angel.spark.ml.graph.algo.embedding.line2._
import com.tencent.angel.spark.ml.util.LogUtils
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class DocVecModel(params: DocVecParam) extends Word2VecModel(params) {

  def action(corpus: RDD[(String, Array[Int])], params: Word2VecParam): DataFrame = {
    var startTs = System.currentTimeMillis()
    // Before training, checkpoint the model
    psMatrix.checkpoint(0)
    LogUtils.logTime(s"Write checkpoint use time=${System.currentTimeMillis() - startTs}")
    startTs = System.currentTimeMillis()
    val result = corpus.mapPartitions{ iterator =>
      iterator.sliding(params.batchSize, params.batchSize)
        .map(batch => batchAction(batch.toArray, params.asInstanceOf[DocVecParam].poolingStrategy))
    }.flatMap(x => x)
    println("compute finished, time cost: " + (System.currentTimeMillis()-startTs) / 1000.0f)
    println("sample some result:")
    result.take(5).foreach(x => println(x))
    SparkSession.builder().getOrCreate().createDataFrame(result.map(x => Row(x)),
      StructType(Seq(StructField("DocVec", StringType, nullable = false))))
  }

  def batchAction(batch: Array[(String, Array[Int])], poolingStrategy: String): Iterator[String] = {
    incCallNum()
    var start = 0L
    start = System.currentTimeMillis()
    val feats = psMatrix.asyncPsfGet(new LINEGetSrcEmbedding(new LINEGetSrcEmbeddingParam(matrixId, batch.flatMap(x => x._2))))
      .get(600000, TimeUnit.MILLISECONDS).asInstanceOf[LINEGetSrcEmbeddingResult].getResult
    incPullTime(start)
    start = System.currentTimeMillis()
    val result = new ArrayBuffer[String]()
    var emb: Array[Double] = null
    for (ele <- batch) {
      val docId = ele._1
      if (poolingStrategy.equals("SUM") || poolingStrategy.equals("MEAN")) {
        emb = poolingSumMean(ele._2, feats)
      } else if (poolingStrategy.equals("MAX") || poolingStrategy.equals("MIN")) {
        emb = poolingMaxMin(ele._2, feats)
      }
      result.append(docId + " " + emb.mkString(" "))
    }
    incCalUpdateTime(start)
    if (totalCallNum % 10 == 0) {
      println(s"avgPullTime=$avgPullTime avgPoolingTime=$avgCalTime")
    }
    result.toIterator
  }


  def poolingSumMean(batch: Array[Int], feats: Int2ObjectOpenHashMap[Array[Float]]): Array[Double] ={
    val result = new Array[Double](params.embeddingDim)
    for (nodeId <- batch) {
      val feat = feats.get(nodeId)
      result.indices.foreach(i => result(i) += feat(i))
    }
    if (params.poolingStrategy.equals("MEAN")) {
      result.indices.foreach(i => result(i) = result(i) / batch.length)
    }
    result
  }

  def poolingMaxMin(batch: Array[Int], feats: Int2ObjectOpenHashMap[Array[Float]]): Array[Double] ={
    val result = feats.get(batch(0))
    for (i <- 1 until batch.length) {
      val feat = feats.get(batch(i))
      if (params.poolingStrategy.equals("MAX")) {
        result.indices.foreach(j => result(j) = math.max(result(j), feat(j)))
      } else if(params.poolingStrategy.equals("MIN")) {
        result.indices.foreach(j => result(j) = math.min(result(j), feat(j)))
      }
    }
    result.map(x => x.toDouble)
  }

}
