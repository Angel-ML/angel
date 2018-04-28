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

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, SparseVector, Vector}
import com.tencent.angel.spark.ml.classification.SparseLRModel
import com.tencent.angel.spark.ml.optimize.{FTRL, FTRLWithVRG}
import com.tencent.angel.spark.ml.util._
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

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

  // decide the way of optimize between ftrl and ftrl_vrg
  val OPT_METHOD = "optMethod"
  val FTRL = "ftrl"
  val FTRL_VRG = "ftrlVRG"
  val BATCH2_SAVE = "batch2Save"
  val IS_ONE_HOT = "isOneHot"

  val MODEL_PATH = "modelPath"
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
    val alpha = params.getOrElse(ALPHA, "1.0").toDouble
    val beta = params.getOrElse(BETA, "1.0").toDouble
    val lambda1 = params.getOrElse(LAMBDA1, "1.0").toDouble
    val lambda2 = params.getOrElse(lAMBDA2, "1.0").toDouble
    val rho1 = params.getOrElse(RHO1, "1.0").toDouble
    val rho2 = params.getOrElse(RHO2, "1.0").toDouble
    val dim = params.getOrElse("dim", "11").toLong
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "3").toInt
    val streamingWindow = params.getOrElse(STREAMING_WINDOW, "60").toInt
    val modelPath = params.getOrElse(MODEL_PATH, null)
    val logPath = params.getOrElse(LOG_PATH, null)
    val checkPointPath = params.getOrElse(CHECK_POINT_PATH, null)
    val zkQuorum = params.getOrElse(ZK_QUORUM, null)
    val topic = params.getOrElse(TOPIC, null)
    val group = params.getOrElse(GROUP, null)
    val optMethod = params.getOrElse(OPT_METHOD, FTRL)
    val isIncrementLearn = params.getOrElse(IS_INCREMENT_LEARN, "false").toBoolean
    val isOneHot = params.getOrElse(IS_ONE_HOT, "true").toBoolean
    val batch2Save = params.getOrElse(BATCH2_SAVE, "10").toInt
    val receiverNum = params.getOrElse(RECEIVER_NUM, "4").toInt

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
        val nPS: SparsePSVector = PSVector.duplicate(zPS)
        // init the model
        if (isIncrementLearn) {
          println("this is increment learning")
          val ZNModel = incLearnZNModel(sc, modelPath)
          zPS.increment(new SparseVector(dim, ZNModel._1))
          nPS.increment(new SparseVector(dim, ZNModel._2))
        }

        val ftrl = new FTRL(lambda1, lambda2, alpha, beta)
        ftrl.initPSModel(dim)

        train(ftrl, featureDS, dim, partitionNum, modelPath, batch2Save, isOneHot)

      case FTRL_VRG =>

        val zPS: SparsePSVector = PSVector.sparse(dim)
        val nPS: SparsePSVector = PSVector.duplicate(zPS)
        val vPS: SparsePSVector = PSVector.duplicate(zPS)
        var initW: SparseVector = null

        // increment learn from original model or just init the w and z model
        if (isIncrementLearn) {
          println("this is increment learning")
          val ZNVWModel = incLearnZNVWModel(sc, modelPath)
          zPS.increment(new SparseVector(dim, ZNVWModel._1))
          nPS.increment(new SparseVector(dim, ZNVWModel._2))
          vPS.increment(new SparseVector(dim, ZNVWModel._3))

          initW = new SparseVector(dim, ZNVWModel._4)

        } else {
          // randomly initialize the w and z model
          val randomW = randomInit(dim).toArray
          val initZInc = randomInit(dim).toArray

          println("random w is:" + randomW.mkString(SPACE_SPLITER))
          println("random z is:" + initZInc.mkString(SPACE_SPLITER))

          initW = new SparseVector(dim, randomW)
          zPS.increment(new SparseVector(dim, initZInc))
        }

        val ftrlVRG = new FTRLWithVRG(lambda1, lambda2, alpha, beta, rho1, rho2)
        ftrlVRG.initPSModel(dim)

        train(ftrlVRG, initW, featureDS, dim, partitionNum, modelPath, batch2Save, isOneHot)
      }
      // start to create the job
      ssc.start()
      // await for application stop
      ssc.awaitTermination()
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

  // train by ftrl
  def train(ftrl: FTRL,
            featureDS: DStream[String],
            dim: Long,
            partitionNum: Int,
            modelPath: String,
            batch2Save: Int,
            isOneHot: Boolean) = {

    var numBatch = 0
    featureDS.foreachRDD { labelFeatRdd =>

      numBatch += 1
      var is2Save = false
      if(batch2Save != 0 && numBatch % batch2Save == 0 ){
        is2Save = true
      }

      val aveLossRdd = labelFeatRdd.repartition(partitionNum)
        .mapPartitions{ dataIter =>
          val dataCollects = dataIter.toArray

          if(dataCollects.length != 0){
            val dataVector = dataCollects.map(x => parseData(x, dim, isOneHot))
            val batchAveLoss = ftrl.optimize(dataVector, calcGradientLoss)
            Iterator(batchAveLoss)
          }else{
            Iterator()
          }
        }

      val globalAveLoss = aveLossRdd.collect

      // save the information to hdfs for persistence
      if(globalAveLoss.length != 0){

        val globalLoss = globalAveLoss.sum / globalAveLoss.length

        println("the current average loss is:" + globalLoss)
        Infor2HDFS.saveLog2HDFS(globalLoss.toString)
      }

      if(is2Save){
        val wModel = SparseLRModel(ftrl.weight)
        println(s"batch: $numBatch model info: ${wModel.simpleInfo}")
        wModel.save(modelPath)
      }
    }
  }

  // train by ftrl_VRG
  def train(ftrlVRG: FTRLWithVRG,
            initW: SparseVector,
            featureDS: DStream[String],
            dim: Long,
            partitionNum: Int,
            modelPath: String,
            batch2Save: Int,
            isOneHot: Boolean) = {

    var localW = initW
    var numBatch = 0
    featureDS.foreachRDD { labelFeatRdd =>

      numBatch += 1
      var is2Save = false
      if(batch2Save != 0 && numBatch % batch2Save == 0 ){
        is2Save = true
      }

      val aveLossRdd = labelFeatRdd.repartition(partitionNum)
        .mapPartitions{ dataIter =>

          val dataCollects = dataIter.toArray
          if(dataCollects.length != 0){

            val dataVector = dataCollects.map(x => parseData(x, dim, isOneHot))
            val wAndLoss = ftrlVRG.optimize(dataVector, localW, calcGradientLoss)
            localW = wAndLoss._1

            Iterator(wAndLoss._2)
          }else{
            Iterator()
          }
        }

      val globalAveLoss = aveLossRdd.collect

      if(globalAveLoss.length != 0){

        val globalLoss = globalAveLoss.sum / globalAveLoss.length

        println("the current average loss is:" + globalLoss)
        Infor2HDFS.saveLog2HDFS(globalLoss.toString)
      }

      if(is2Save){
        val wModel = SparseLRModel(ftrlVRG.weight)
        println(s"batch: $numBatch model info: ${wModel.simpleInfo}")
        wModel.save(modelPath)
      }
    }
  }

  def parseData(dataStr: String, dim: Long, isOneHot: Boolean): (Vector, Double) = {

    if(!isOneHot) {

      // SparseVector
      val (feature, label) = DataLoader.transform2Sparse(dataStr)
      val featAddInter =  Array((0L, 1.0)) ++ feature
      val featV: Vector = new SparseVector(dim.toLong, featAddInter)

      (featV, label)
    } else {
      // OneHotVector
      val (feature, label) = DataLoader.transform2OneHot(dataStr)
      val featAddInter = 0L +: feature
      val featV: Vector = new OneHotVector(dim.toLong, featAddInter)

      (featV, label)
    }

  }

  private def calcLoss(w: SparseVector, label: Double, feature: Vector): Double = {
    val margin = -1 * BLAS.dot(w, feature)
    val loss = if (label > 0) {
      math.log1p(math.exp(margin))
    } else {
      math.log1p(math.exp(margin)) - margin
    }
    loss
  }

  private def calcGradientLoss(w: SparseVector, label: Double, feature: Vector): (SparseVector, Double) = {
    val margin = -1 * BLAS.dot(w, feature)
    val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
    val grad = new SparseVector(w.length)
    BLAS.axpy(gradientMultiplier, feature, grad)

    val loss = if (label > 0) {
      math.log1p(math.exp(margin))
    } else {
      math.log1p(math.exp(margin)) - margin
    }

    (grad, loss)
  }

}
