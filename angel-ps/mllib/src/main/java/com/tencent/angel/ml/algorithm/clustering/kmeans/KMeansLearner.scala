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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.clustering.kmeans

import java.util
import java.util.{Random, _}

import com.tencent.angel.ml.algorithm.MLLearner
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.worker.storage.{MemoryStorage, Storage}
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
  // Number of features
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, 10000)
  // Number of epoch
  val epochNum: Int = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  // Number of samples per batch
  val batch_sample_num: Int = conf.getInt(MLConf.ML_BATCH_SAMPLE_NUM, 100)

  LOG.info(s"Start KMeans learner, K=${K}, #Feature=${feaNum}, #Iteration=${epochNum}, " +
    s"#SamplePerMiniBatch=${batch_sample_num}")

  // Create a Kmeans Model according to the conf
  val kmeansModel = new KmeansModel(conf, ctx)

  /**
    * Train a KMeans Model
    *
    * @param trainData : trainning dataset storage
    */
  override
  def train(trainData: Storage[LabeledData]) = {
    // Nubmber of samples for each cluster
    val spCountPerCenter = new Array[Int](K)

    // Init cluster centers randomly
    initKCentersRandomly(trainData)

    // Learn KMeans Model iteratively, apply a mini-batch updation in each iteration
    while (ctx.getIteration < epochNum) {
      //LOG.info("CTX.GETITERATION= " +ctx.getIteration)
      var startEpoch = System.currentTimeMillis()
      trainOneEpoch(ctx.getIteration, trainData, spCountPerCenter)
      var epochTime = System.currentTimeMillis() - startEpoch

      var startObj = System.currentTimeMillis()
      computeObjValue(trainData, ctx.getIteration)
      var objTime = System.currentTimeMillis() - startObj

      LOG.info(s"Task[${ctx.getContext.getIndex}] Iteration[${ctx.getIteration}] finish. " +
        s"mini-batch cost $epochTime ms, compute " +
        s"objective value cost $objTime ms")

      ctx.increaseIteration()
    }

  }

  /**
    * Pick up K samples as initial centers randomly, and push them to PS.
    *
    * @param dataStorage : trainning data storage, the cluster center candidates
    */
  def initKCentersRandomly(dataStorage: Storage[LabeledData]): Unit = {
    LOG.info("Initialize cluster centers with randomly choosen samples.")
    val start = System.currentTimeMillis()
    val rand = new Random(System.currentTimeMillis())

    for (i <- 0 until K) {
      if (i % ctx.getTotalTaskNum == ctx.getTaskId.getIndex) {
        val newCent = dataStorage.get(rand.nextInt(dataStorage.getTotalElemNum())).getX
          .asInstanceOf[TDoubleVector]
        newCent.setRowId(i)
        kmeansModel.centers.increment(newCent)
      }
    }

    kmeansModel.centers.clock().get()

    // Wait for all workers finish push centers to PS
    ctx.globalSync(ctx.getMatrix(kmeansModel.centers.getName).getMatrixId)

    LOG.info(s"Init cluster centers cost ${System.currentTimeMillis() - start} ms")
  }

  def pickSamples(dataStorage: Storage[LabeledData], num: Int): Storage[LabeledData] = {
    val random = new Random(System.currentTimeMillis())
    val candidates = new ArrayList[Int]()
    for (i <- 0 to dataStorage.getTotalElemNum)
      candidates.add(i, i)

    val spStorage = new MemoryStorage[LabeledData](-1)

    for (i <- 0 until num) {
      if (i % ctx.getTotalTaskNum == ctx.getTaskId.getIndex) {
        val rand = random.nextInt(candidates.size())
        val spId = candidates.get(rand)

        val newCent = dataStorage.get(spId).getX.asInstanceOf[TDoubleVector]
        newCent.setRowId(i)
        kmeansModel.centers.increment(newCent)
        candidates.remove(rand)
        LOG.info(s"Task[${ctx.getContext.getIndex}]  choose the ${spId}-th sample.${newCent.get(0)}, ${newCent.get(1)}, " + s"${newCent.get(2)}")
      }
    }

    spStorage
  }

  def updateCenters(oldCenters: ArrayList[TDoubleVector]): Unit = {
    for (i <- 0 until K) {
      val oldCenter = oldCenters.get(i)
      oldCenter.timesBy(-1)
      oldCenter.plusBy(kmeansModel.lcCenters.get(i), 1)
      oldCenter.setRowId(i)

      kmeansModel.centers.increment(oldCenter)
    }
  }


  /**
    * Each epoch updation is performed in three steps. First, pull the centers updated by last
    * epoch from PS. Second, a mini batch of size batch_sample_num is sampled. Third, update the
    * centers in a mini-batch way.
    *
    * @param trainData              : the trainning data storage
    * @param per_center_step_counts : the array caches the number of samples per center
    */
  def trainOneEpoch(epoch: Int, trainData: Storage[LabeledData], per_center_step_counts:
  Array[Int]): Unit = {
    val startEpoch = System.currentTimeMillis()

    // Pull centers from PS.
    kmeansModel.pullCentersFromPS()
    val pullCost = System.currentTimeMillis() - startEpoch

    // Back up centers for delta computation
    val oldCenters = new util.ArrayList[TDoubleVector](K)
    for (i <- 0 until K) {
      oldCenters.add(kmeansModel.lcCenters.get(i).asInstanceOf[DenseDoubleVector].clone())
    }

    // Pick up #batch_sample_num sampels randomly
    val bSamples = picInstances(trainData)

    // Run Mini-batch update
    var startMbatch = System.currentTimeMillis()
    miniBatchUpdation(bSamples, per_center_step_counts)
    var batchCost = System.currentTimeMillis() - startMbatch

    // Push updation to PS
    var startUpdate = System.currentTimeMillis()
    updateCenters(oldCenters)
    var updateCost = System.currentTimeMillis() - startUpdate

    kmeansModel.centers.clock().get()

    var epochCost = System.currentTimeMillis() - startEpoch
    LOG.info(s"Task[${ctx.getContext.getIndex}] Iteration[$epoch] pull centers from PS cost $pullCost " +
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
  def miniBatchUpdation(samples: Storage[LabeledData], per_center_step_counts: Array[Int]) {
    // A map sotres the samples of each cneter, the key is the center id and the value is the
    // samples array.
    val mini_batch_centers = new HashMap[Integer, IntArrayList]

    for (i <- 0 until samples.getTotalElemNum) {
      val cId_minDis = kmeansModel.findClosestCenter(samples.get(i).getX
        .asInstanceOf[TDoubleVector])
      if (!mini_batch_centers.contains(cId_minDis._1))
        mini_batch_centers.put(cId_minDis._1, new IntArrayList)
      mini_batch_centers(cId_minDis._1).add(i)
    }

    // Each sample is used to update its closest center with learning rate eta, eta is updated
    // by the number of samples of the center: \eta = 1 / count[c]
    for ((cId, spIDs) <- mini_batch_centers) {
      var m = 1.0
      for (i <- 0 until spIDs.size()) {
        val spID = spIDs.getInt(i)
        per_center_step_counts(cId) += 1
        val c = 1.0F
        val eta = c / (per_center_step_counts(cId) + c)
        m *= 1.0 - eta
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
  def picInstances(trainData: Storage[LabeledData]): Storage[LabeledData] = {
    // Pick #sampleBatch samples randomly
    val rand = new Random(System.currentTimeMillis())
    val batchSample = new MemoryStorage[LabeledData](-1)

    for (i <- 0 until batch_sample_num) {
      batchSample.put(trainData.get(rand.nextInt(trainData.getTotalElemNum())))
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
  def computeObjValue(dataStorage: Storage[LabeledData], epoch: Int): Unit = {
    val clstCenters = kmeansModel.findClosestCenter(dataStorage)
    var obj = 0.0
    for (i <- 0 until clstCenters.size())
      obj += clstCenters.get(i)._2

    val objVec = new DenseDoubleVector(epochNum)
    objVec.set(epoch, obj)
    objVec.setRowId(0)

    kmeansModel.objMat.increment( objVec)
    kmeansModel.objMat.clock().get()

    val averObj = kmeansModel.objMat.getRow(0).get(epoch)


    LOG.info(s"Task[${ctx.getContext.getIndex}] Iteration[$epoch] local objective value=$obj, global average " +
      s"objective value=$averObj")
  }

}
