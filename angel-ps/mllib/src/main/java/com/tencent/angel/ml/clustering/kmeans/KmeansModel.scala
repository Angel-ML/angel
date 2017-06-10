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

import java.io.DataOutputStream
import java.util

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.clustering.kmeans.KmeansModel._
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, TDoubleVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

/**
  * Kmeans Model is a clustering model, find closest center to each point.
  */
object KmeansModel {
  val KMEANS_CENTERS_MAT = "kmeans_centers"
  val KMEANS_OBJ = "kmeans_obj"
}

class KmeansModel(_ctx: TaskContext, conf: Configuration) extends MLModel(_ctx) {

  def this(conf: Configuration) = {
    this(null, conf)
  }

  // Number of clusters
  private val K: Int = conf.getInt(MLConf.KMEANS_CENTER_NUM, 3)
  // Number of features
  private val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // Number of epoch
  private val epoch: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)

  // Reference for centers matrix on PS server
  val centers = new PSModel[TDoubleVector](KMEANS_CENTERS_MAT, K, feaNum)
  centers.setAverage(true)
  addPSModel(KMEANS_CENTERS_MAT, centers)

  // Reference for objective value matrix on PS server
  val objMat = new PSModel[DenseDoubleVector](KMEANS_OBJ, 1, epoch)
  addPSModel(KMEANS_OBJ, objMat)

  // Centers pulled to local worker
  var lcCenters : util.List[TDoubleVector] = new util.ArrayList[TDoubleVector]()
  setLoadPath(conf)
  setSavePath(conf)

  /**
    * Pull centers from PS to local worker
    */
  def pullCentersFromPS(): Unit = {
    val rowIndexes = new Array[Int](K)
    for (i <- 0 until K)
      rowIndexes(i) = i
    lcCenters = centers.getRows(rowIndexes)
  }

  /**
    * Set the save path of cluster centers
    * After trainning, centers will be saved to HDFS
    *
    * @param conf : configuration of algorithm and resource
    */
  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
    if (path != null)
      centers.setSavePath(path)
  }

  /**
    * Set the load path of cluster centers
    * Before training, centers will be loaded from HDFS
    *
    * @param conf : configuration of algorithm and resource
    */
  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    if (path != null)
      centers.setLoadPath(path)
  }

  /**
    * @return : Number of clusters
    */
  def getK = K

  /**
    * Find the closest center for predict instances
    *
    * @param storage : predict data storage
    * @return : predict result storage
    */
  override
  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    pullCentersFromPS()

    val predictResult = new MemoryDataBlock[PredictResult](-1)

    storage.resetReadIndex()
    var data: LabeledData = null
    for (i <- 0 until storage.getTotalElemNum) {
      data = storage.read()
      val cid = findClosestCenter(data.getX.asInstanceOf[TDoubleVector])._1
      predictResult.put(new kmeansResult(data.getY, cid))
    }

    predictResult
  }

  /**
    * Calculate the distance between instance and centers, and find the closest center
    *
    * @param x : a instance
    * @return : the closest center id
    */
  def findClosestCenter(x: TDoubleVector): Tuple2[Integer, Double] = {
    var minDis = Double.MaxValue
    var clstCent: Int = -1

    for (i <- 0 until K) {
      val dist = distanceToCenterId(i, x)
      if (dist < minDis) {
        minDis = dist
        clstCent = i
      }
    }
    new Tuple2[Integer, Double](clstCent, minDis)
  }

  def findClosestCenter(instances: DataBlock[LabeledData]): util.ArrayList[Tuple2[Integer, Double]] = {
    pullCentersFromPS()
    val clstCenter = new util.ArrayList[Tuple2[Integer, Double]]

    for (i <- 0 until instances.getTotalElemNum) {
      val pre = findClosestCenter(instances.get(i).getX.asInstanceOf[TDoubleVector])
      clstCenter.add(pre)
    }

    clstCenter
  }

  /**
    * Calculate distanve between a sample and a center
    *
    * @param cId centerID
    * @param x   sample
    * @return distance from sample to center
    */
  def distanceToCenterId(cId: Int, x: TDoubleVector): Double = {
    // || a - b || ^ 2 = a^2 - 2ab + b^2
    //    x.squaredNorm - 2 * center.dot(x) + center.squaredNorm
    x.squaredNorm - 2 * lcCenters.get(cId).dot(x) + lcCenters.get(cId).squaredNorm
  }
}

/**
  * Predict result of kmeans
  */
class kmeansResult(instanceId: Double, centerId: Int) extends PredictResult {

  override def writeText(output: DataOutputStream): Unit = {
    output.writeBytes(instanceId + PredictResult.separator + centerId)
  }
}
