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


package com.tencent.angel.spark.ml

import java.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.online_learning.{FTRLRunner, FTRLWithVRG}
import com.tencent.angel.spark.ml.util.Infor2HDFS
import com.tencent.angel.ml.math2.VFactory

object OnLineLRWithFTRLTest {

  def main(args: Array[String]): Unit = {

    val topic = "20180428"
    val zkQuorum = "localhost:2181"
    val group = "ftrltest"
    val lambda1 = 0.1
    val lambda2 = 0.2
    val alpha = 0.1
    val beta = 1.0
    val rho1 = 0.1
    val rho2 = 0.1
    val dim = 5
    val partitionNum = 1
    val modelSavePath = "./spark-on-angel/mllib/src/test/data/model_path"
    val logPath = "./spark-on-angel/mllib/src/test/data/log_path"
    val batch2Save = 10

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FTRLTest")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")

    val topicMap: Map[String, Int] = Map(topic -> 1)
    val featureDS = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    Infor2HDFS.initLogPath(ssc, logPath)

    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      val ftrlVRG = new FTRLWithVRG(lambda1, lambda2, alpha, beta, rho1, rho2)
      ftrlVRG.initPSModel(dim)

      featureDS.print()

      val initW = VFactory.sparseLongKeyDoubleVector(dim, Array(1L, 2L), Array(0.1, 0.2))
      FTRLRunner.train(ftrlVRG, initW, featureDS, dim, partitionNum, modelSavePath, batch2Save, true)
      // start to create the job
      ssc.start()
      // await for application stop
      ssc.awaitTermination()

    }

  }


  def runSpark(name: String)(body: SparkContext => Unit): Unit = {
    println("this is in run spark")
    val conf = new SparkConf
    val master = conf.getOption("spark.master")
    val isLocalTest = if (master.isEmpty || master.get.toLowerCase.startsWith("local")) true else false
    val sparkBuilder = SparkSession.builder().appName(name)
    if (isLocalTest) {
      sparkBuilder.master("local")
        .config("spark.ps.mode", "LOCAL")
        .config("spark.ps.jars", "")
        .config("spark.ps.instances", "1")
        .config("spark.ps.cores", "1")
    }
    val sc = sparkBuilder.getOrCreate().sparkContext
    body(sc)
    val wait = sys.props.get("spark.local.wait").exists(_.toBoolean)
    if (isLocalTest && wait) {
      println("press Enter to exit!")
      Console.in.read()
    }
    sc.stop()
  }

  def randomInit(dim: Long): Map[Long, Double] = {
    val selectNum = if (dim < 10) dim.toInt else 10
    val dimReFact = if (dim <= Int.MaxValue) dim.toInt else Int.MaxValue
    var resultRandom: Map[Long, Double] = Map()
    val randGene = new Random()

    (0 until selectNum).foreach { i =>
      val randomId = randGene.nextInt(dimReFact - 1).toLong
      val randomVal = 10 * randGene.nextDouble() + 1.0

      resultRandom += (randomId -> randomVal)
    }
    // indices 0 is not in our feature
    resultRandom.filter(x => x._1 > 0)
  }
}
