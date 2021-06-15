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

import java.text.DecimalFormat
import java.util

import com.tencent.angel.ml.clustering.kmeans.KMeansModel._
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntFloatVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


object KMeansModel {
  val KMEANS_CENTERS_MAT = "kmeans_centers"
  val KMEANS_V_MAT = "kmeans_v"
  val KMEANS_OBJ = "kmeans_obj"
}

class KMeansModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  private val indexRange: Long = SharedConf.indexRange
  // Number of features
  val modelSize: Long = SharedConf.modelSize
  private val K: Int = conf.getInt(MLConf.KMEANS_CENTER_NUM, MLConf.DEFAULT_KMEANS_CENTER_NUM) // Number of clusters

  // Centers pulled to local worker
  var lcCenters: util.List[Vector] = new util.ArrayList[Vector]()
  var lcV: IntFloatVector = VFactory.denseFloatVector(K)
  val centerDist = new Array[Double](K)

  // Reference for centers matrix on PS server
  val centers: PSModel = new PSModel(KMEANS_CENTERS_MAT, K, indexRange, -1, -1, modelSize).setAverage(true).setRowType(SharedConf.modelType)
  val v: PSModel = new PSModel(KMEANS_V_MAT, 1, K, -1, -1).setAverage(true).setRowType(RowType.T_FLOAT_DENSE)

  addPSModel(KMEANS_CENTERS_MAT, centers)
  addPSModel(KMEANS_V_MAT, v)
  setSavePath(conf)
  setLoadPath(conf)

  /**
   * Pull centers from PS to local worker
   */
  def pullCentersFromPS(): Unit = {
    lcCenters = centers.getRows((0 until K).toArray)

    centerDist.indices.foreach { i =>
      val c = lcCenters.get(i)
      centerDist(i) = c.dot(c)
    }
  }

  /**
   * Pull mini-batch samples from PS to local worker
   */
  def pullVFromPS(): Unit = {
    lcV = v.getRow(0).asInstanceOf[IntFloatVector]
  }

  /**
   * @return : Number of clusters
   */
  def getK: Int = K

  /**
   * Predict use the PSModels and predict data
   *
   * @param storage predict data
   * @return predict result
   */
  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    pullCentersFromPS()
    pullVFromPS()
    val predictResult = new MemoryDataBlock[PredictResult](-1)
    val silhouetteFlag = conf.getBoolean(MLConf.KMEANS_SILHOUETTE_FLAG, false)
    storage.resetReadIndex()
    var data: LabeledData = null
    for (_ <- 0 until storage.size) {
      data = storage.read()
      val predictResultTmp = findClosestCenter(data.getX)
      val cid = predictResultTmp._1
      val dist = predictResultTmp._2
      val silhouette = predictResultTmp._3
      predictResult.put(KMeansResult(data.getAttach, cid, dist = dist, silhouette = silhouette, silhouetteFlag = silhouetteFlag))
    }

    predictResult
  }

  /**
   * Calculate the distance between instance and centers, and find the closest center
   *
   * @param x : a instance
   * @return : the closest center id
   */
  def findClosestCenter(x: Vector): (Int, Double, Double) = {
    var minDis = Double.MaxValue
    var clstCent: Int = -1
    val silhouetteFlag = conf.getBoolean(MLConf.KMEANS_SILHOUETTE_FLAG, false)
    var silhouetteACount = 0.0 //the samples count of the cluster that x Vector belongs to
    var silhouetteBSum = 0.0 //all sum distance
    var silhouetteBCount = 0.0 // all samples number
    var silhouette = 0.0

    val len2 = x.dot(x)
    for (i <- 0 until K) {
      val dist = centerDist(i) - 2 * lcCenters.get(i).dot(x) + len2
      var samplesNumber = 0.0f

      if (silhouetteFlag) {
        samplesNumber = lcV.get(i)
        silhouetteBSum += Math.sqrt(dist) * samplesNumber
        silhouetteBCount += samplesNumber
      }

      if (dist < minDis) {
        minDis = dist
        clstCent = i
        silhouetteACount = samplesNumber
      }
    }

    val silhouetteMinDisSqrt = Math.sqrt(minDis)

    if (silhouetteFlag) {
      val bFinal = (silhouetteBSum - silhouetteACount * silhouetteMinDisSqrt) / (silhouetteBCount - silhouetteACount)
      silhouette = (bFinal - silhouetteMinDisSqrt) / {
        if (bFinal > silhouetteMinDisSqrt) bFinal else silhouetteMinDisSqrt
      }
    }
    (clstCent, silhouetteMinDisSqrt, silhouette)
  }

  def updateCenterDist(idx: Int): Unit = {
    val cent: Vector = lcCenters.get(idx)
    centerDist(idx) = cent.dot(cent)
  }
}

case class KMeansResult(sid: String, pred: Double, label: Double = Double.NaN, dist: Double, silhouette: Double, silhouetteFlag: Boolean) extends PredictResult {
  val df = new DecimalFormat("0")

  override def getText: String = {
    if (silhouetteFlag) {
      sid + separator + format.format(pred) + separator + format.format(dist) + separator + format.format(silhouette)
    }
    else {
      sid + separator + format.format(pred) + separator + format.format(dist)
    }

  }
}
