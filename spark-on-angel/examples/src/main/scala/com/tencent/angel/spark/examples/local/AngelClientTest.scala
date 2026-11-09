package com.tencent.angel.spark.examples.local

import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.embedding.line2.{LINEGetEmbedding, LINEGetEmbeddingParam, LINEGetEmbeddingResult}
import org.apache.spark.SparkConf

import java.util.concurrent.TimeUnit

object AngelClientTest {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val psMasterIp = params.getOrElse("psMasterIp", "11.60.244.74")
    val psMasterPort = params.getOrElse("psMasterPort", "21996").toInt
    val conf = new SparkConf()
    val psAgent = new PSAgent(psMasterIp, psMasterPort, 200)
    psAgent.initAndStart()
    val matrix = psAgent.getMatrixClient("embedding")
    val result = matrix.asyncGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrix.getMatrixId, Array(1,2,3), Array(4,5,6),
      Array(Array(8,9), Array(10,11), Array(12,13)), 2, 2))).get(600000, TimeUnit.MILLISECONDS).asInstanceOf[LINEGetEmbeddingResult].getResult
    println("oyw test, " + result._1.get(1).mkString("Array(", ", ", ")"))
    println("oyw test, " + result._2.get(4).mkString("Array(", ", ", ")"))
  }

}