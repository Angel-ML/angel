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

import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.ml.optimize.{FTRL, FTRLWithVRG}
import com.tencent.angel.spark.ml.util.Infor2HDFS
import com.tencent.angel.spark.models.vector.SparsePSVector
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object FTRLLearner{

  val SPACE_SPLITTER: String = " "
  val P_LABEL = 1.0
  val Z = "z"
  val N = "n"
  val V = "v"
  val W = "w"

  // train by ftrl_vrg
  def train(zPS: SparsePSVector,
            nPS: SparsePSVector,
            vPS: SparsePSVector,
            initW: Map[Long, Double],
            featureDS: DStream[String],
            dim: Long,
            alpha: Double,
            beta: Double,
            lambda1: Double,
            lambda2: Double,
            rho1: Double,
            rho2: Double,
            partitionNum: Int,
            modelPath: String,
            batch2Check: Int,
            batch2Save: Int) = {

    var numBatch = 0
    var aveBatLoss = 0.0
    var localW = initW

    featureDS.foreachRDD { labelFeatRdd =>

      val rePartitionedRdd = labelFeatRdd.repartition(partitionNum)

      numBatch += 1
      // whether to check the model
      var is2Check = false
      if (batch2Check != 0 && numBatch % batch2Check == 0) {
        println("this is numBatch:" + numBatch)
        is2Check = true
        rePartitionedRdd.cache()
      }

      // whether to save the model
      var is2Save = false
      if(batch2Save != 0 && numBatch % batch2Save == 0){
        is2Save = true
      }


      // update the local weight by moving average between local weight and new weight
      val newAndLocW = FTRLWithVRG.getNewWeight(
        zPS,
        nPS,
        localW,
        rho1,
        alpha,
        beta,
        lambda1,
        lambda2)

      val newWeight = newAndLocW._1
      localW = newAndLocW._2

      val lossPartRdd = rePartitionedRdd.mapPartitionsWithIndex { (partitionId, dataIter) =>

        var lossTotal = 0.0
        var newGradTotal: Map[Long, Double] = Map()
        var localGradTotal: Map[Long, Double] = Map()
        val dataCollects = dataIter.toArray
        val dataNum = dataCollects.length

        // all data in this partiton compute the gradient and finally get the average
        dataCollects.foreach { dataStr =>

          val (label, featureIdVal) = spareLabelFeature(dataStr)

          // compute the loss for the new sample
          val loss = computeLoss(label, featureIdVal, localW)
          lossTotal += loss

          val newGradient = getGradLoss(newWeight, label, featureIdVal)
          val localGradient = getGradLoss(localW, label, featureIdVal)

          newGradTotal = incrementVector(newGradTotal, newGradient)
          localGradTotal = incrementVector(localGradTotal, localGradient)
        }

        // update the z and n model on PS
        FTRLWithVRG.updateZN(
          zPS,
          nPS,
          vPS,
          dim,
          newWeight,
          alpha,
          beta,
          lambda1,
          lambda2,
          rho1,
          rho2,
          newGradTotal,
          localGradTotal,
          1.0 / dataNum
        )

        Iterator((lossTotal, dataNum))
      }

      // compute the global loss
      val globalInf = lossPartRdd.collect

      val globalLoss = globalInf.map(_._1).sum
      val totalNum = globalInf.map(_._2).sum.toDouble

      // write the average loss to hdfs
      if (totalNum != 0) {
        aveBatLoss = globalLoss / totalNum
        println("the globalLoss is:" + aveBatLoss)

        Infor2HDFS.saveLog2HDFS(aveBatLoss.toString)
      }

      if(is2Save){

        // just pull w model to save
        val zModelPS = sparsePSModel(zPS)
        val nModelPS = sparsePSModel(nPS)
        val vModelPS = sparsePSModel(vPS)
        val wModelPS = localW

        val zModel = Array(Z) ++ zModelPS.map(x => x._1 + ":" + x._2)
        val nModel = Array(N) ++ nModelPS.map(x => x._1 + ":" + x._2)
        val vModel = Array(V) ++ vModelPS.map(x => x._1 + ":" + x._2)
        val wModel = Array(W) ++ wModelPS.map(x => x._1 + ":" + x._2)

        val message = Array(zModel, nModel, vModel, wModel).map(_.mkString(SPACE_SPLITTER))
        Infor2HDFS.saveModel2HDFS(modelPath, message)
      }


      // check the loss and auc for each spot and save the model
      if (is2Check) {
        // check the current loss and auc
        val spotLossAUC = rePartitionedRdd.map { dataStr =>
          val (label, featureIdVal) = spareLabelFeature(dataStr)
          val sampleLoss = computeLoss(label, featureIdVal, localW)
          val predictVal = sigmoid(dot(localW.toArray, featureIdVal))

          (label, sampleLoss, predictVal)
        }.collect()

        // compute the average loss and auc
        val lengBatch = spotLossAUC.length.toDouble
        if (lengBatch != 0) {
          val aveLossSpot = spotLossAUC.map(_._2).sum / lengBatch
          val aucSpot = computeAUC(spotLossAUC.map(x => (x._1, x._3)), P_LABEL)
          Infor2HDFS.saveLog2HDFS("the spot loss and auc is:" + aveLossSpot + SPACE_SPLITTER + aucSpot)
        }

        rePartitionedRdd.unpersist(false)
      }

    }
  }


  // train by ftrl
  def train(
             zPS: SparsePSVector,
             nPS:SparsePSVector,
             featureDS: DStream[String],
             dim: Long,
             alpha: Double,
             beta: Double,
             lambda1: Double,
             lambda2: Double,
             partitionNum: Int,
             modelPath: String,
             batch2Check: Int,
             batch2Save: Int) = {

    var numBatch = 0
    var aveBatLoss = 0.0
    featureDS.foreachRDD { labelFeatRdd =>

      val rePartitionedRdd = labelFeatRdd.repartition(partitionNum)

      numBatch += 1

      var is2Check = false
      if(batch2Check != 0 && numBatch % batch2Check == 0){
        println("this is numBatch:" + numBatch)
        is2Check = true
        rePartitionedRdd.cache()
      }

      var is2Save = false
      if(batch2Save != 0 && numBatch % batch2Save == 0 ){
        is2Save = true
      }

      val incrementRdd = rePartitionedRdd
        .mapPartitionsWithIndex { (partitionId, dataIter) =>

          var lossTotal = 0.0
          val dataCollects = dataIter.toArray
          val dataNum = dataCollects.length

          // init the increment of model for z and n
          var incrementZ: Map[Long, Double] = Map()
          var incrementN: Map[Long, Double] = Map()

          // pull the z and n model to executor
          var localZ = sparsePSModel(zPS)
          var localN = sparsePSModel(nPS)

          dataCollects.foreach{ dataStr =>

            val (label, featureIdVal) = spareLabelFeature(dataStr)

            // update weight and compute the loss for new sample
            val loss = computeLoss(label, featureIdVal, localZ, localN, alpha, beta, lambda1, lambda2)
            lossTotal += loss

            // train
            val updateIncZN = FTRL.trainByInstance(
              (label, featureIdVal),
              localZ,
              localN,
              alpha,
              beta,
              lambda1,
              lambda2,
              getGradLoss
            )

            // add the increment of z and n model,z and n model will be change in this function
            incrementZ = incrementVector(incrementZ, updateIncZN._1)
            incrementN = incrementVector(incrementN, updateIncZN._2)

            localZ = incrementVector(localZ, updateIncZN._1)
            localN = incrementVector(localN, updateIncZN._2)

          }

          // return the increment of z and n
          Iterator(
            Tuple3(("z" + partitionId, incrementZ),
              ("n" + partitionId, incrementN),
              (lossTotal, dataNum))
          )
        }.cache()

      incrementRdd.count

      // push all the increment to z and n
      val increPush = incrementRdd.mapPartitions { increZNLIte =>

        var lossParTotal = 0.0
        var sampleNum = 0

        while (increZNLIte.hasNext) {
          val increZNL = increZNLIte.next()
          zPS.increment(new SparseVector(dim, increZNL._1._2.toArray))
          nPS.increment(new SparseVector(dim, increZNL._2._2.toArray))

          lossParTotal += increZNL._3._1
          sampleNum += increZNL._3._2
        }
        Iterator((lossParTotal, sampleNum))
      }

      val globalInf = increPush.collect

      val globalLoss = globalInf.map(_._1).sum
      val totalNum = globalInf.map(_._2).sum.toDouble

      // write average loss for each batch to hdfs
      if(totalNum != 0){
        aveBatLoss = globalLoss / totalNum
        println("the current average loss is:" + aveBatLoss)
        Infor2HDFS.saveLog2HDFS(aveBatLoss.toString)
      }

      incrementRdd.unpersist(false)

      if(is2Save){

        // save model
        val zModelPS = sparsePSModel(zPS)
        val nModelPS = sparsePSModel(nPS)

        val zModel = Array(Z) ++ zModelPS.map(x => x._1 + ":" + x._2)
        val nModel = Array(N) ++ nModelPS.map(x => x._1 + ":" + x._2)
        // compute the w model
        val wModelPS = FTRLLearner.getNewWeight(zModelPS, nModelPS, alpha, beta, lambda1, lambda2)
        val wModel = Array(W) ++ wModelPS.map(x => x._1 + ":" + x._2)

        val message = Array(zModel, nModel, wModel).map(_.mkString(SPACE_SPLITTER))
        Infor2HDFS.saveModel2HDFS(modelPath, message)
      }

      // check the loss and auc for each spot and save the model
      if(is2Check){

        val zModelPS = sparsePSModel(zPS)
        val nModelPS = sparsePSModel(nPS)

        // check the current loss and auc
        val spotLossAUC = rePartitionedRdd.map{ dataStr =>
          val (label, featureIdVal) = spareLabelFeature(dataStr)
          val sampleLoss = computeLoss(label, featureIdVal, zModelPS, nModelPS, alpha, beta, lambda1, lambda2)
          val weight = getWeight(featureIdVal, zModelPS, nModelPS, alpha, beta, lambda1, lambda2)
          val predictVal = sigmoid(dot(weight, featureIdVal))

          (label, sampleLoss, predictVal)
        }.collect()

        // compute the average loss and auc
        val lengBatch = spotLossAUC.length.toDouble
        if(lengBatch != 0){
          val aveLossSpot = spotLossAUC.map(_._2).sum / lengBatch
          val aucSpot = computeAUC(spotLossAUC.map(x => (x._1, x._3)), P_LABEL)
          Infor2HDFS.saveLog2HDFS("the spot loss and auc is:" + aveLossSpot + SPACE_SPLITTER + aucSpot)
        }

        rePartitionedRdd.unpersist(false)
      }

    }
  }

  // add updateInc to incrementedVec
  def incrementVector(incrementedVec: Map[Long, Double],
                      updateInc: Map[Long, Double]): Map[Long, Double] = {

    var vecAddResult: Map[Long, Double] = incrementedVec

    updateInc.foreach { case (fId, fInc) =>
      val oriVal = vecAddResult.getOrElse(fId, 0.0)
      val newVal = oriVal + fInc
      vecAddResult += (fId -> newVal)
    }

    vecAddResult
  }

  // compute the increment of oriModel, newModel - oriModel
  def subtractVector(oriModel: Map[Long, Double], newModel: Map[Long, Double]): Map[Long, Double] = {

    val oriSet = oriModel.keySet
    val newSet = newModel.keySet
    (oriSet ++ newSet).map { fId =>
      val oriVal = oriModel.getOrElse(fId, 0.0)
      val newVal = newModel.getOrElse(fId, 0.0)
      (fId, newVal - oriVal)
    }.toMap
  }

  // get the gradient of loss function
  def getGradLoss(w: Map[Long, Double],
                  label: Double,
                  feature: Array[(Long, Double)]): Map[Long, Double] = {

    val p = sigmoid(dot(w.toArray, feature))
    feature.map(x => (x._1, (p - label) * x._2)).toMap
  }

  // the moving for average
  def moveAverage(localVec: Map[Long, Double],
                  newVec: Map[Long, Double],
                  k: Double): Map[Long, Double] = {
    incrementVector(kMulVec(k, localVec), kMulVec(1.0 - k, newVec))
  }

  // k * vector
  def kMulVec(k: Double, vec: Map[Long, Double]): Map[Long, Double] = {
    vec.map { case (id, value) => (id, k * value) }
  }

  // sigmoid function: 1 / (1 + exp(-z))
  def sigmoid(value: Double): Double = {
    val pre = 1 / (1 + Math.exp(-1 * value))
    pre
  }

  // two vector dot, first sort the vector according to the indices
  def dot(x: Array[(Long, Double)], y: Array[(Long, Double)]): Double = {

    val xSorted = x.sortBy(_._1)
    val ySorted = y.sortBy(_._1)

    val xValues = xSorted.map(_._2)
    val xIndices = xSorted.map(_._1)
    val yValues = ySorted.map(_._2)
    val yIndices = ySorted.map(_._1)

    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }

    sum
  }

  def predictInstance(feature: Array[(Long, Double)],
                      localW: Map[Long, Double]): Double = {

    val weight = localW.toArray
    // w * x
    sigmoid(dot(weight, feature))
  }


  def save(dataRDD: RDD[Array[String]], path: String): Unit = {
    val outputPath = new Path(path)
    val conf = dataRDD.context.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    dataRDD.map(_.mkString(SPACE_SPLITTER)).saveAsTextFile(path)

  }

  // compute Log loss, label is 0 or 1
  // -1 * loss is for minimize
  def computeLoss(label: Double,
                  feature: Array[(Long, Double)],
                  localW: Map[Long, Double]): Double = {

    val dotVal = dot(localW.toArray, feature)
    val loss = label * dotVal - Math.log(1 + Math.exp(dotVal))

    -1 * loss
  }

  // compute Log loss, label is 0 or 1
  def computeLoss(label: Double,
                  feature: Array[(Long, Double)],
                  localZ: Map[Long, Double],
                  localN: Map[Long, Double],
                  alpha: Double,
                  beta: Double,
                  lambda1: Double,
                  lambda2: Double): Double = {

    val weight = getWeight(feature, localZ, localN, alpha, beta, lambda1, lambda2)
    val dotVal = dot(weight, feature)
    val loss = label * dotVal - Math.log(1 + Math.exp(dotVal))

    -1 * loss
  }

  def getWeight(feature: Array[(Long, Double)],
                localZ: Map[Long, Double],
                localN: Map[Long, Double],
                alpha: Double,
                beta: Double,
                lambda1: Double,
                lambda2: Double): Array[(Long, Double)] = {

    // the number of not zero of feature
    val featLeg = feature.length
    val localW = new Array[(Long, Double)](featLeg)

    (0 until featLeg).foreach { i =>

      val fId = feature(i)._1
      val zVal = localZ.getOrElse(fId, 0.0)
      val nVal = localN.getOrElse(fId, 0.0)
      // w_local的更新
      localW(i) = (fId, FTRL.updateWeight(zVal, nVal, alpha, beta, lambda1, lambda2))
    }

    localW
  }

  def spareLabelFeature(dataStr: String):(Double, Array[(Long, Double)]) = {
    val labelFeature = dataStr.split(SPACE_SPLITTER)
    val label = labelFeature(0).toDouble
    val feature = labelFeature.tail
    val featureIdVal = feature.map { idVal =>
      val idValArr = idVal.split(":")
      (idValArr(0).toLong, idValArr(1).toDouble)
    }
    (label, featureIdVal)
  }

  // compute the auc of result
  def computeAUC(result: Array[(Double, Double)], posLabel: Double): Double = {

    // the number of positive and negative sample
    val M = result.count(_._1 == posLabel)
    val N = result.count(_._1 != posLabel)

    // sorted and ordered from 1 and filter the positive samples
    val posRankSum = result
      .sortBy(x => x._2)
      .zipWithIndex
      .map(x => (x._1, x._2 + 1))
      .filter(x => x._1._1 == posLabel)
      .map(x => x._2)
      .sum

    (posRankSum - (M * (M + 1) / 2)).toDouble / (M * N)
  }

  // sparse model from ps
  def sparsePSModel(psVec: SparsePSVector): Map[Long, Double] = {
    val localModel = psVec.pull.pairs
    val modelInd = localModel._2
    val modelVal = localModel._3
    modelInd.zip(modelVal).toMap
  }

  // compute new weight
  def getNewWeight(feature: Array[(Long, Double)],
                   zModel: Map[Long, Double],
                   nModel: Map[Long, Double],
                   alpha: Double,
                   beta: Double,
                   lambda1: Double,
                   lambda2: Double): Map[Long, Double] = {

    var wModel: Map[Long, Double] = Map()

    feature.foreach{ case (id, value) =>
      val zVal = zModel.getOrElse(id, 0.0)
      val nVal = nModel.getOrElse(id, 0.0)
      val wVal = if (Math.abs(zVal) <= lambda1)
        0.0
      else
        (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal - Math.signum(zVal).toInt * lambda1)

      wModel += (id -> wVal)
    }

    wModel
  }

  // compute new weight no matter the feature
  def getNewWeight(zModel: Map[Long, Double],
                   nModel: Map[Long, Double],
                   alpha: Double,
                   beta: Double,
                   lambda1: Double,
                   lambda2: Double): Map[Long, Double] = {

    var wModel: Map[Long, Double] = Map()

    zModel.foreach { case (id, value) =>
      val nVal = nModel.getOrElse(id, 0.0)

      // exit new weight
      if (Math.abs(value) > lambda1) {
        val wVal = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (value - Math.signum(value).toInt * lambda1)
        wModel += (id -> wVal)
      }
    }

    wModel
  }

}