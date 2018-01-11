/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.ml.online_learning

import java.util.Random

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.ml.util.{ActionType, ArgsUtil, Infor2HDFS, ParamKeys}
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * this module is to run sparse lr with ftrl or ftrl_vrg,
  * the window for ftrlwithSVRG should be small for accurate,
  * assume each batch is a sample
  */

object FTRLRunner {

  //init param parameters for alpha, beta, lambda1, lambda2
  val ALPHA = "alpha"
  val BETA = "beta"
  val LAMBDA1 = "lambda1"
  val lAMBDA2 = "lambda2"
  val RHO = "rho"
  val RHO1 = "rho1"
  val RHO2 = "rho2"
  val SEPARATOR = "separator"
  val CHECK_POINT_PATH = "checkPointPath"
  val SPACE_SPLITER = " "
  val ZK_QUORUM = "zkQuorum"
  val TOPIC = "topic"
  val GROUP = "group"
  val TID = "tid"

  // decide the way of optimize between ftrl and ftrl_vrg
  val OPT_METHOD = "optMethod"
  val FTRL = "ftrl"
  val FTRL_VRG = "ftrlVRG"
  val BATCH2_CHECK = "batch2Check"
  val BATCH2_SAVE = "batch2Save"

  val MODEL_SAVE_PATH = "modelSavePath"
  val RECEIVER_NUM = "receiverNum"
  val STREAMING_WINDOW = "streamingWindow"
  val LOG_PATH = "logPath"
  val IS_INCREMENT_LEARN = "isIncrementLearn"

  val Z = "z"
  val N = "n"
  val V = "v"
  val W = "w"

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, ActionType.TRAIN)
    val alpha = params.getOrElse(ALPHA, "1.0").toDouble
    val beta = params.getOrElse(BETA, "1.0").toDouble
    val lambda1 = params.getOrElse(LAMBDA1, "1.0").toDouble
    val lambda2 = params.getOrElse(lAMBDA2, "1.0").toDouble
    val rho1 = params.getOrElse(RHO1, "1.0").toDouble
    val rho2 = params.getOrElse(RHO2, "1.0").toDouble
    val dim = params.getOrElse("dim", "11").toLong
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "3").toInt
    val streamingWindow = params.getOrElse(STREAMING_WINDOW, "60").toInt
    val modelSavePath = params.getOrElse(MODEL_SAVE_PATH, null)
    val logPath = params.getOrElse(LOG_PATH, null)
    val checkPointPath = params.getOrElse(CHECK_POINT_PATH, null)
    val zkQuorum = params.getOrElse(ZK_QUORUM, null)
    val topic = params.getOrElse(TOPIC, null)
    val group = params.getOrElse(GROUP, null)
    val tid = params.getOrElse(TID, null)
    val optMethod = params.getOrElse(OPT_METHOD, FTRL)
    val input = params.getOrElse(ParamKeys.INPUT, null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val sampleRate = params.getOrElse(ParamKeys.SAMPLE_RATE, "1.0").toDouble
    val isIncrementLearn = params.getOrElse(IS_INCREMENT_LEARN, "false").toBoolean
    val batch2Check = params.getOrElse(BATCH2_CHECK, "0").toInt
    val batch2Save = params.getOrElse(BATCH2_SAVE, "10").toInt
    val receiverNum = params.getOrElse(RECEIVER_NUM, "4").toInt

    if (actionType == ActionType.TRAIN) {
      val sparkConf = new SparkConf().setAppName("SparseLRFTRL")
      val ssc = new StreamingContext(sparkConf, Seconds(streamingWindow))
      ssc.checkpoint(checkPointPath)
      // should not ignored
      val sc = ssc.sparkContext
      PSContext.getOrCreate(sc)

      // for kafka mode
      val topicMap: Map[String, Int] = Map(topic -> receiverNum)
      val featureDS = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      // init the log path
      Infor2HDFS.initLogPath(ssc, logPath)

      optMethod match {
        case FTRL =>

          val zPS: SparsePSVector = PSVector.sparse(dim)
          val nPS: SparsePSVector = PSVector.sparse(dim)
          // init the model
          if(isIncrementLearn){
            println("this is increment learning")
            val ZNModel = incLearnZNModel(sc, modelSavePath)
            zPS.increment(new SparseVector(dim, ZNModel._1))
            nPS.increment(new SparseVector(dim, ZNModel._2))
          }

          FTRLLearner.train(
            zPS,
            nPS,
            featureDS,
            dim,
            alpha,
            beta,
            lambda1,
            lambda2,
            partitionNum,
            modelSavePath,
            batch2Check,
            batch2Save)

        case FTRL_VRG =>

          val zPS: SparsePSVector = PSVector.sparse(dim)
          val nPS: SparsePSVector = PSVector.sparse(dim)
          val vPS: SparsePSVector = PSVector.sparse(dim)
          var initW: Map[Long, Double] = Map()

          // increment learn from original model or just init the w and z model
          if(isIncrementLearn){
            println("this is increment learning")
            val ZNVWModel = incLearnZNVWModel(sc, modelSavePath)
            zPS.increment(new SparseVector(dim, ZNVWModel._1))
            nPS.increment(new SparseVector(dim, ZNVWModel._2))
            vPS.increment(new SparseVector(dim, ZNVWModel._3))

            initW = ZNVWModel._4.toMap

          }else{
            // randomly initialize the w and z model
            initW = randomInit(dim)
            val initZInc = randomInit(dim).toArray

            println("random w is:" + initW.mkString(SPACE_SPLITER))
            println("random z is:" + initZInc.mkString(SPACE_SPLITER))

            zPS.increment(new SparseVector(dim, initZInc))
          }

          FTRLLearner.train(
            zPS,
            nPS,
            vPS,
            initW,
            featureDS,
            dim,
            alpha,
            beta,
            lambda1,
            lambda2,
            rho1,
            rho2,
            partitionNum,
            modelSavePath,
            batch2Check,
            batch2Save)
      }
      // start to create the job
      ssc.start()
      // await for application stop
      ssc.awaitTermination()
    } else {
      val sparkConf = new SparkConf().setAppName("SparseFTRLTest")
      val sc = SparkContext.getOrCreate(sparkConf)

      // parse the model of z and n
      val modelLocal = sc.textFile(modelSavePath).collect()
      val wModel = sparseModel(modelLocal, W)
      val parseRDD = sc.textFile(input)
        .sample(false, sampleRate)
        .repartition(partitionNum)
        .map(line => line.trim)
        .filter(_.nonEmpty)
        .map { dataStr =>
          val labelFeature = dataStr.split(SPACE_SPLITER)
          val feature = labelFeature.tail
          val featureIdVal = feature.map { idVal =>
            val idValArr = idVal.split(":")
            (idValArr(0).toLong, idValArr(1).toDouble)
          }
          Array(dataStr, FTRLLearner.predictInstance(featureIdVal, wModel.toMap).toString)
        }

      // save the predict rdd
      FTRLLearner.save(parseRDD, output)
    }
  }

  // parse the z and n model
  def incLearnZNModel(sc:SparkContext,
                      modelPath: String): (Array[(Long, Double)], Array[(Long, Double)]) = {

    val modelStr = sc.textFile(modelPath).collect()

    val zModel = sparseModel(modelStr, Z)
    val nModel = sparseModel(modelStr, N)

    (zModel, nModel)

  }

  def incLearnZNVWModel(sc:SparkContext,
                        modelPath: String): (Array[(Long, Double)], Array[(Long, Double)], Array[(Long, Double)], Array[(Long, Double)]) = {

    val modelStr = sc.textFile(modelPath).collect()

    val zModel = sparseModel(modelStr, Z)
    val nModel = sparseModel(modelStr, N)
    val vModel = sparseModel(modelStr, V)
    val wModel = sparseModel(modelStr, W)

    (zModel, nModel, vModel, wModel)
  }


  def sparseModel(modelStr:Array[String], flag: String):Array[(Long, Double)] = {
    modelStr.filter(str => str.contains(flag))(0)
      .split(SPACE_SPLITER)
      .tail
      .map{idVal =>
        val idValArr = idVal.split(":")
        (idValArr(0).toLong, idValArr(1).toDouble)
      }
  }

  // randomly initialize the model according to the dim
  // inorder to keep the sparse,just init 10 feature at most
  def randomInit(dim: Long): Map[Long, Double] = {
    val selectNum = if(dim < 10) dim.toInt else 10
    val dimReFact = if(dim <= Int.MaxValue ) dim.toInt else Int.MaxValue
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

}
