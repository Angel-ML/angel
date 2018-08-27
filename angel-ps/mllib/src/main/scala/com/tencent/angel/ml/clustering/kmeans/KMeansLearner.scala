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


package com.tencent.angel.ml.clustering.kmeans

import java.util
import java.util.{ArrayList, Random}

import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}


class KMeansLearner(ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[KMeansLearner])

  // Number of features
  val indexRange: Long = SharedConf.indexRange
  // Number of epoch
  val epochNum: Int = SharedConf.epochNum
  // Number of clusters
  val K: Int = conf.getInt(MLConf.KMEANS_CENTER_NUM, MLConf.DEFAULT_KMEANS_CENTER_NUM)
  // Adaptive learning rate parameter
  val C: Double = conf.getDouble(MLConf.KMEANS_C, MLConf.DEFAULT_KMEANS_C)

  LOG.info(s"Task[${ctx.getTaskIndex}] Start KMeans learner, K=$K, C=$C, #Feature=$indexRange, " +
    s"#Iteration=$epochNum")

  // Create a Kmeans Model according to the conf
  val kmeansModel = new KMeansModel(conf, ctx)

  /**
    * Train a ML Model
    *
    * @param trainData : input train data storage
    * @param valiData  : validate data storage
    * @return : a learned model
    */
  override def train(trainData: DataBlock[LabeledData], valiData: DataBlock[LabeledData]): MLModel = {
    // Number of samples for each cluster
    val spCountPerCenter = new Array[Int](K)

    if (SharedConf.useShuffle) {
      trainData.shuffle()
    }

    // Init cluster centers randomly
    if (ctx.getTaskId.getIndex == 0) {
      initKCentersRandomly(trainData)
    } else {
      kmeansModel.centers.syncClock()
    }

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(trainData.size))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(valiData.size))
    // Learn KMeans Model iteratively, apply a mini-batch update in each iteration
    while (ctx.getEpoch < epochNum) {
      val startEpoch = System.currentTimeMillis()
      trainOneEpoch(ctx.getEpoch, trainData, spCountPerCenter)
      val epochTime = System.currentTimeMillis() - startEpoch

      val startObj = System.currentTimeMillis()
      val localObj = computeObjValue(trainData, ctx.getEpoch)
      val objTime = System.currentTimeMillis() - startObj

      val valiObj = computeObjValue(valiData, ctx.getEpoch)

      LOG.info(s"Task[${ctx.getContext.getIndex}] Iter=${ctx.getEpoch} success. "
        + s"totalloss=$localObj. mini-batch cost $epochTime ms, compute " +
        s"obj cost $objTime ms")

      globalMetrics.metric(MLConf.TRAIN_LOSS, localObj)
      globalMetrics.metric(MLConf.VALID_LOSS, valiObj)
      ctx.incEpoch()
    }

    kmeansModel
  }

  /**
    * Pick up K samples as initial centers randomly, and push them to PS.
    *
    * @param dataStorage : trainning data storage, the cluster center candidates
    */
  def initKCentersRandomly(dataStorage: DataBlock[LabeledData]): Unit = {
    LOG.info(s"Task[${ctx.getContext.getIndex}] Initialize cluster centers with randomly choosen " +
      "samples.")
    val start = System.currentTimeMillis()
    val rand = new Random(System.currentTimeMillis())

    for (i <- 0 until K) {
      if (i % ctx.getTotalTaskNum == ctx.getTaskId.getIndex) {
        val newCent = dataStorage.get(rand.nextInt(dataStorage.size)).getX
        newCent.setRowId(i)
        kmeansModel.centers.increment(newCent)
      }
    }

    kmeansModel.centers.syncClock()

    LOG.info(s"All tasks Init cluster centers success, cost ${System.currentTimeMillis() - start}" +
      s" ms")
  }

  /**
    * Each epoch updation is performed in three steps. First, pull the centers updated by last
    * epoch from PS. Second, a mini batch of size batch_sample_num is sampled. Third, update the
    * centers in a mini-batch way.
    *
    * @param trainData              : the trainning data storage
    * @param per_center_step_counts : the array caches the number of samples per center
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], per_center_step_counts: Array[Int]): Unit = {
    val startEpoch = System.currentTimeMillis()

    // 1. Pull centers ang counter from PS.
    kmeansModel.pullCentersFromPS()
    kmeansModel.pullVFromPS()
    val pullCost = System.currentTimeMillis() - startEpoch

    // 2. Back up centers for delta computation
    val oldCenters = new util.ArrayList[Vector](K)
    (0 until K).foreach { i =>
      oldCenters.add(kmeansModel.lcCenters.get(i).copy())
    }
    val oldVs = kmeansModel.lcV.copy()

    val batchNum = SharedConf.numUpdatePerEpoch
    val batchSize = (trainData.size() + batchNum - 1) / batchNum
    val batchData = new Array[LabeledData](batchSize)
    val iter = batchGenerator(trainData, batchData, batchNum)

    // 4. Run Mini-batch update
    val startMbatch = System.currentTimeMillis()
    while (iter.hasNext) {
      iter.next()

      batchData.foreach { ld =>
        val cId = kmeansModel.findClosestCenter(ld.getX)._1
        kmeansModel.lcV.set(cId, kmeansModel.lcV.get(cId) + 1)
        val eta = C / (kmeansModel.lcV.get(cId) + C)
        kmeansModel.lcCenters.get(cId).imul(1 - eta).iaxpy(ld.getX, eta)
        kmeansModel.updateCenterDist(cId)
      }
    }
    var batchCost = System.currentTimeMillis() - startMbatch

    // Push updation to PS
    val startUpdate = System.currentTimeMillis()
    updateCenters(oldCenters, oldVs)
    val updateCost = System.currentTimeMillis() - startUpdate

    val epochCost = System.currentTimeMillis() - startEpoch
    LOG.debug(s"Task[${ctx.getContext.getIndex}] Iteration[$epoch] pull centers from PS cost " +
      s"$pullCost ms, push centers to PS cost $updateCost")

  }

  def updateCenters(oldCenters: ArrayList[Vector], oldVs: Vector): Unit = {
    for (i <- 0 until K) {
      val centerUpdate = kmeansModel.lcCenters.get(i).sub(oldCenters.get(i))
      kmeansModel.centers.increment(centerUpdate)
    }
    val counterUpdate = kmeansModel.lcV.sub(oldVs)
    kmeansModel.v.increment(counterUpdate)

    kmeansModel.centers.syncClock()
    kmeansModel.v.syncClock()
  }

  /**
    * Pick up #batch_sample_num samples randomly from the trainning data.
    *
    * @param trainData
    * @return
    */
  def batchGenerator(trainData: DataBlock[LabeledData], batchData: Array[LabeledData], batchNum: Int
                    ): Iterator[Unit] = {

    new Iterator[Unit] {
      private var count: Int = 0

      override def hasNext: Boolean = count < batchNum

      override def next(): Unit = {
        val pivot = Math.random()
        batchData.indices.foreach { i =>
          var flag = true
          while (flag) {
            val tmpVector = trainData.loopingRead()
            if (Math.random() < pivot) {
              batchData(i) = tmpVector
              flag = false
            }
          }
        }
        count += 1
      }
    }
  }

  /**
    * Compute the objective values of all samples, which is measured by the distance from a
    * sample to its closest center.
    *
    * @param dataStorage : the trainning dataset
    * @param epoch       : the epoch number
    */
  def computeObjValue(dataStorage: DataBlock[LabeledData], epoch: Int): Double = {
    var obj = 0.0
    dataStorage.resetReadIndex()
    (0 until dataStorage.size()).foreach { _ =>
      val vec = dataStorage.read().getX
      val cIdDist = kmeansModel.findClosestCenter(vec)
      obj += cIdDist._2
    }
    obj
  }
}
