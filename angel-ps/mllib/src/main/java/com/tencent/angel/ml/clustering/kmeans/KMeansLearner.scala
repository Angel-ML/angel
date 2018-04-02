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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.clustering.kmeans

import java.util
import java.util.{Random, _}

import com.tencent.angel.ml.MLLearner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TIntDoubleVector}
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.ints.IntArrayList
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.HashMap

/**
  * Kmeans is clustering algorithm, which find the closest center of each instance.
  * This is the learner class of kmeans.
  *
  * @param ctx : context of this task
  */
class KMeansLearner(override val ctx: TaskContext) extends MLLearner(ctx) {

  val LOG: Log = LogFactory.getLog(classOf[KMeansLearner])

  // Number of clusters
  val K: Int = conf.getInt(MLConf.KMEANS_CENTER_NUM, 2)
  // Adaptive learnning rate parameter
  val C = conf.getDouble(MLConf.kMEANS_C, 0.1);
  // Number of features
  val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, -1L)
  assert(indexRange != -1L)
  // Number of epoch
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  // Number of samples per batch
  val spRatio: Double = conf.getDouble(MLConf.KMEANS_SAMPLE_RATIO_PERBATCH, 0.5)

  LOG.info(s"Task[${ctx.getTaskIndex}] Start KMeans learner, K=$K, C=$C, #Feature=$indexRange, " +
    s"#Iteration=$epochNum, #SampleRaioPerMiniBatch=$spRatio")

  // Create a Kmeans Model according to the conf
  val kmeansModel = new KMeansModel(conf, ctx)

  /**
    * Train a KMeans Model
    *
    * @param trainData : trainning dataset storage
    */
  override
  def train(trainData: DataBlock[LabeledData], valiData: DataBlock[LabeledData]): MLModel = {

    // Nubmber of samples for each cluster
    val spCountPerCenter = new Array[Int](K)

    // Init cluster centers randomly
    initKCentersRandomly(trainData)

    globalMetrics.addMetric("global.obj", LossMetric(trainData.size))
    // Learn KMeans Model iteratively, apply a mini-batch updation in each iteration
    while (ctx.getEpoch < epochNum) {

      val startEpoch = System.currentTimeMillis()
      trainOneEpoch(ctx.getEpoch, trainData, spCountPerCenter)
      val epochTime = System.currentTimeMillis() - startEpoch

      val startObj = System.currentTimeMillis()
      val localObj = computeObjValue(trainData, ctx.getEpoch)
      val objTime = System.currentTimeMillis() - startObj

      LOG.info(s"Task[${ctx.getContext.getIndex}] Iter=${ctx.getEpoch} success. "
        + s"localObj=$localObj. mini-batch cost $epochTime ms, compute " +
        s"obj cost $objTime ms")

      globalMetrics.metric("global.obj", localObj)
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
        val r = rand.nextInt(dataStorage.size - 1) + 1

        var data: LabeledData = null
        for (i <- 0 until r) {
          data = dataStorage.read()

          if (data == null) {
            dataStorage.resetReadIndex()
            data = dataStorage.read()
          }
        }

        val newCent = data.getX.asInstanceOf[TIntDoubleVector]
        newCent.setRowId(i)
        kmeansModel.centers.increment(newCent)
      }
    }

    kmeansModel.centers.syncClock()

    // Wait for all workers finish push centers to PS
    ctx.globalSync(ctx.getMatrix(kmeansModel.centers.modelName).getMatrixId)

    LOG.info(s"All tasks Init cluster centers success, cost ${System.currentTimeMillis() - start}" +
      s" ms")
  }

  def updateCenters(oldCenters: ArrayList[TIntDoubleVector]): Unit = {
    for (i <- 0 until K) {
      val oldCenter = oldCenters.get(i)
      oldCenter.timesBy(-1)
      oldCenter.plusBy(kmeansModel.lcCenters.get(i), 1)
      oldCenter.setRowId(i)
      kmeansModel.centers.increment(oldCenter)
    }

    kmeansModel.centers.syncClock()
  }


  /**
    * Each epoch updation is performed in three steps. First, pull the centers updated by last
    * epoch from PS. Second, a mini batch of size batch_sample_num is sampled. Third, update the
    * centers in a mini-batch way.
    *
    * @param trainData              : the trainning data storage
    * @param per_center_step_counts : the array caches the number of samples per center
    */
  def trainOneEpoch(epoch: Int, trainData: DataBlock[LabeledData], per_center_step_counts:
  Array[Int]): Unit = {
    val startEpoch = System.currentTimeMillis()

    // Pull centers from PS.
    kmeansModel.pullCentersFromPS()
    val pullCost = System.currentTimeMillis() - startEpoch

    // Back up centers for delta computation
    val oldCenters = new util.ArrayList[TIntDoubleVector](K)
    for (i <- 0 until K) {
      oldCenters.add(kmeansModel.lcCenters.get(i).asInstanceOf[DenseDoubleVector].clone())
    }

    val batchSize = (trainData.size * spRatio).asInstanceOf[Int]

    val sampleBatch = picInstances(trainData)

    // Run Mini-batch update
    var startMbatch = System.currentTimeMillis()
    miniBatchUpdation(sampleBatch, per_center_step_counts)
    var batchCost = System.currentTimeMillis() - startMbatch

    // Push updation to PS
    var startUpdate = System.currentTimeMillis()
    updateCenters(oldCenters)
    var updateCost = System.currentTimeMillis() - startUpdate

    var epochCost = System.currentTimeMillis() - startEpoch
    LOG.debug(s"Task[${ctx.getContext.getIndex}] Iteration[$epoch] pull centers from PS cost " +
      s"$pullCost " +
      s"ms, " +
      s"mini-batch cost" +
      s" " +
      s"$batchCost ms, update cost $updateCost ms.")

  }

  /**
    * Upate the centers with a mini batch samples, first find the closest center for each sample.
    * Second each sample is used to update its closest center using the per-center learning rate.
    *
    * @param samples                : the samples picked up for mini batch updation
    * @param per_center_step_counts : the array sotres the number of samples of each center
    */
  def miniBatchUpdation(samples: DataBlock[LabeledData], per_center_step_counts: Array[Int]) {
    // A map sotres the samples of each cneter, the key is the center id and the value is the
    // samples array.
    val mini_batch_centers = new HashMap[Integer, IntArrayList]

    var data: LabeledData = null
    samples.resetReadIndex()
    for (i <- 0 until samples.size) {
      data = samples.read

      val cId_minDis = kmeansModel.findClosestCenter(samples.get(i).getX
        .asInstanceOf[TIntDoubleVector])
      if (!mini_batch_centers.contains(cId_minDis._1))
        mini_batch_centers.put(cId_minDis._1, new IntArrayList)
      mini_batch_centers(cId_minDis._1).add(i)
    }

    //TODO DELET
    for ((cId, spIDs) <- mini_batch_centers) {
      LOG.debug(s"center$cId has ${spIDs.size()} samples.")
    }

    // Each sample is used to update its closest center with learning rate eta, eta is updated
    // by the number of samples of the center: \eta = 1 / count[c]
    for ((cId, spIDs) <- mini_batch_centers) {
      var m = 1.0
      for (i <- 0 until spIDs.size()) {
        val spID = spIDs.getInt(i)
        per_center_step_counts(cId) += 1
        val eta = C / (per_center_step_counts(cId) + C)
        m *= (1.0 - eta)
        kmeansModel.lcCenters.get(cId).plusBy(samples.get(spID).getX, eta)
      }

      kmeansModel.lcCenters.get(cId).timesBy(m)
    }
  }

  /**
    * Pick up #batch_sample_num samples randomly from the trainning data.
    *
    * @param trainData
    * @return
    */
  def picInstances(trainData: DataBlock[LabeledData]): DataBlock[LabeledData] = {
    // Pick #spRatio ratio samples randomly
    val batch_sample_num = (spRatio * trainData.size).asInstanceOf[Int]
    val batchSample = new MemoryDataBlock[LabeledData](-1)

    var data: LabeledData = null
    for (i <- 0 until batch_sample_num) {
      data = trainData.read()
      if (data == null) {
        trainData.resetReadIndex()
        data = trainData.read()
      }
      batchSample.put(data)
    }

    batchSample
  }

  /**
    * Compute the objective values of all samples, which is measured by the distance from a
    * sample to its closest center.
    *
    * @param dataStorage : the trainning dataset
    * @param epoch       : the epoch number
    */
  def computeObjValue(dataStorage: DataBlock[LabeledData], epoch: Int): Double = {
    val clstCenters = kmeansModel.findClosestCenter(dataStorage)
    var obj = 0.0
    for (i <- 0 until clstCenters.size())
      obj += clstCenters.get(i)._2

    obj
  }

}
