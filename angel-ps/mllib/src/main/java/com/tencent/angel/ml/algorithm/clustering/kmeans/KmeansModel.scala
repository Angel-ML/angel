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

import java.io.DataOutputStream
import java.text.DecimalFormat
import java.util
import java.util.List

import com.tencent.angel.conf.{AngelConfiguration, MatrixConfiguration}
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{MemoryStorage, Storage}
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.ml.algorithm.clustering.kmeans.KmeansModel._
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory

/**
  * Kmeans Model is a clustering model, find closest center to each point.
  */
object KmeansModel {
  val KMEANS_CENTERS_MAT = "kmeans_centers"
  val KMEANS_OBJ = "kmeans_obj"
}

class KmeansModel(conf: Configuration, ctx: TaskContext) extends AlgorithmModel {

  def this(conf: Configuration) = {
    this(conf, null)
  }

  // Number of clusters
  val K: Int = conf.getInt(MLConf.KMEANS_CENTER_NUM, 2)
  // Number of features
  val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, 10000)
  // Number of epoch
  val epoch: Int = conf.getInt(MLConf.ML_EPOCH_NUM, 10)

  // Reference for centers matrix on PS server
  val centers = new PSModel[TDoubleVector](ctx, KMEANS_CENTERS_MAT, K, feaNum)
  centers.setAverage(true)
  addPSModel(KMEANS_CENTERS_MAT, centers)

  // Reference for objective value matrix on PS server
  val objMat = new PSModel[TDoubleVector](ctx, KMEANS_OBJ, 1, epoch)
  addPSModel(KMEANS_OBJ, objMat)

  // Centers pulled to local worker
  var lcCenters = new util.ArrayList[TDoubleVector]()

  /**
    * Pull centers from PS to local worker
    */
  def pullCentersFromPS() = {
    val rowIDs = new RowIndex();
    for (i <- 0 until K)
      rowIDs.addRowId(i)

    val batchNum: Int = K/ 10
    lcCenters = centers.getRows(rowIDs, batchNum)
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
    centers.setSavePath(path)
  }

  /**
    * Set the load path of cluster centers
    * Before trainning, centers will be loaded from HDFS
    *
    * @param conf : configuration of algorithm and resource
    */
  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    centers.setLoadPath(path)
  }

  /**
    * @return : Number of clusters
    */
  def getK(): Int = {
    K
  }

  /**
    * Find the closest center for predict instances
    *
    * @param storage : predict data storage
    * @return : predict result storage
    */
  override
  def predict(storage: Storage[LabeledData]): Storage[PredictResult] = {
    val predictResult = new MemoryStorage[PredictResult](-1)

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
    var clstCent = -1
    var i = 0

    for (i <- 0 until K) {
      val center = lcCenters.get(i)
      val dist = distanceToCenterId(i, x)
      if (dist < minDis) {
        minDis = dist
        clstCent = i
      }
    }

    new Tuple2[Integer, Double](clstCent, minDis)
  }

  def findClosestCenter(instances: Storage[LabeledData]): util.ArrayList[Tuple2[Integer, Double]]
  = {
    val clstCenter = new util.ArrayList[Tuple2[Integer, Double]]()
    for (i <- 0 until instances.getTotalElemNum) {
      val pre = findClosestCenter(instances.get(i).getX.asInstanceOf[TDoubleVector])
      clstCenter.add(pre)
    }
    clstCenter
  }


  /**
    * Calculate distanve between a sample and a center
    *
    * @param cId
    * @param x
    * @return
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
class kmeansResult(instanceId: Int, centerId: Int) extends PredictResult {

  override def writeText(output: DataOutputStream): Unit = {
    output.writeBytes(instanceId + PredictResult.separator + centerId)
  }
}
