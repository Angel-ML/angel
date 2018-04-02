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
import com.tencent.angel.ml.clustering.kmeans.KMeansModel._
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector.TIntDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

/**
  * Kmeans Model is a clustering model, find closest center to each point.
  */
object KMeansModel {
  val KMEANS_CENTERS_MAT = "kmeans_centers"
  val KMEANS_OBJ = "kmeans_obj"
}

class KMeansModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {


  // Number of clusters
  private val K: Int = conf.getInt(MLConf.KMEANS_CENTER_NUM, 3)
  // Number of features
  private val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, -1L)
  assert(indexRange != -1L)
  val modelSize: Long = conf.getLong(MLConf.ML_MODEL_SIZE, indexRange)
  // Number of epoch
  private val epoch: Int = conf.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  // Centers pulled to local worker
  var lcCenters: util.List[TVector] = new util.ArrayList[TVector]()

  // Reference for centers matrix on PS server
  val centers = new PSModel(KMEANS_CENTERS_MAT, K, indexRange, -1, -1, modelSize).setAverage(true).setRowType(RowType.T_DOUBLE_DENSE)

  addPSModel(KMEANS_CENTERS_MAT, centers)
  setSavePath(conf)
  setLoadPath(conf)


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
    for (i <- 0 until storage.size) {
      data = storage.read()
      val cid = findClosestCenter(data.getX.asInstanceOf[TIntDoubleVector])._1
      predictResult.put(new KMeansResult(data.getY, cid))
    }

    predictResult
  }

  /**
    * Calculate the distance between instance and centers, and find the closest center
    *
    * @param x : a instance
    * @return : the closest center id
    */
  def findClosestCenter(x: TIntDoubleVector): (Int, Double) = {
    var minDis = Double.MaxValue
    var clstCent: Int = -1

    for (i <- 0 until K) {
      val dist = distanceToCenterId(i, x)
      if (dist < minDis) {
        minDis = dist
        clstCent = i
      }
    }
    (clstCent, minDis)
  }

  def findClosestCenter(instances: DataBlock[LabeledData]): util.ArrayList[(Int, Double)] = {
    pullCentersFromPS()
    val clstCenter = new util.ArrayList[(Int, Double)]

    for (i <- 0 until instances.size) {
      val pre = findClosestCenter(instances.get(i).getX.asInstanceOf[TIntDoubleVector])
      clstCenter.add(pre)
    }

    clstCenter
  }

  /**
    * Calculate distanve between a sample and a center
    * || a - b || ^ 2 = a^2 - 2ab + b^^2
    *
    * @param cId centerID
    * @param x   sample
    * @return distance from sample to center
    */
  def distanceToCenterId(cId: Int, x: TIntDoubleVector): Double = {
    x.squaredNorm - 2 * lcCenters.get(cId).dot(x) + lcCenters.get(cId).squaredNorm
  }
}

/**
  * Predict result of kmeans
  */
class KMeansResult(instanceId: Double, centerId: Int) extends PredictResult {

  override def getText(): String = {
    (instanceId + separator + centerId)
  }
}
