package com.tencent.angel.ml.classification.mlr

import java.util

import com.tencent.angel.ml.classification.lr.SparseLRPredictResult
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.MathUtils
import com.tencent.angel.worker.storage.{DataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

/**
  * Created by hbghh on 2017/8/12.
  */

/**
  * MLR model
  *
  */

object MLRModel {
  def apply(conf: Configuration) = {
    new MLRModel(conf)
  }

  def apply(ctx: TaskContext, conf: Configuration) = {
    new MLRModel(conf, ctx)
  }
}

class MLRModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[MLRModel])

  val MLR_SIGMOID_WEIGHT_MAT = "mlr_sigmoid_weight"
  val MLR_SIGMOID_INTERCEPT = "mlr_sigmoid_intercept"

  val MLR_SOFTMAX_WEIGHT_MAT = "mlr_softmax_weight"
  val MLR_SOFTMAX_INTERCEPT = "mlr_softmax_intercept"

  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val rank = conf.getInt(MLConf.ML_MLR_RANK, MLConf.DEFAULT_ML_MLR_RANK)

  val sigmoid_weight = PSModel[DenseDoubleVector](MLR_SIGMOID_WEIGHT_MAT, rank, feaNum).setAverage(true)
  val sigmoid_intercept = PSModel[DenseDoubleVector](MLR_SIGMOID_INTERCEPT, rank, 1).setAverage(true)

  val softmax_weight = PSModel[DenseDoubleVector](MLR_SOFTMAX_WEIGHT_MAT, rank, feaNum).setAverage(true)
  val softmax_intercept = PSModel[DenseDoubleVector](MLR_SOFTMAX_INTERCEPT, rank, 1).setAverage(true)

  addPSModel(MLR_SIGMOID_WEIGHT_MAT, sigmoid_weight)
  addPSModel(MLR_SIGMOID_INTERCEPT, sigmoid_intercept)

  addPSModel(MLR_SOFTMAX_WEIGHT_MAT, softmax_weight)
  addPSModel(MLR_SOFTMAX_INTERCEPT, softmax_intercept)

  setSavePath(conf)
  setLoadPath(conf)


  /**
    *
    * @param dataSet
    * @return
    */
  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    val start = System.currentTimeMillis()

    val (sigmoid_wVecot, sigmoid_b, softmax_wVecot, softmax_b) = pullFromPs()
    val predict = new MemoryDataBlock[PredictResult](-1)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.size) {
      val instance = dataSet.read
      val id = instance.getY
      val softmax = (0 until rank).map(i => softmax_wVecot(i).dot(instance.getX) + softmax_b(i)).toArray
      MathUtils.softmax(softmax)
      val sigmoid = (0 until rank).map(i => MathUtils.sigmoid({
        var temp=sigmoid_wVecot(i).dot(instance.getX) + sigmoid_b(i)
        temp=math.max(temp,-18)
        temp=math.min(temp,18)
        temp
      })).toArray
      val pre = (0 until rank).map(i => softmax(i) * sigmoid(i)).reduce(_ + _)

      predict.put(new MLRPredictResult(id, pre))
    }
    predict
  }

  def pullFromPs() = {
    val start = System.currentTimeMillis()
    val sigmoid_wVecot = new Array[DenseDoubleVector](rank)
    val sigmoid_b = new Array[Double](rank)

    val softmax_wVecot = new Array[DenseDoubleVector](rank)
    val softmax_b = new Array[Double](rank)

    for (i <- 0 until rank) {
      sigmoid_wVecot(i) = sigmoid_weight.getRow(i)
      sigmoid_b(i) = sigmoid_intercept.getRow(i).get(0)

      softmax_wVecot(i) = softmax_weight.getRow(i)
      softmax_b(i) = softmax_intercept.getRow(i).get(0)
    }

    val cost = System.currentTimeMillis() - start
    LOG.info(s"pull MLR Model from PS cost $cost ms.")
    (sigmoid_wVecot, sigmoid_b, softmax_wVecot, softmax_b)
  }

  def pushToPS(update_sigmoid_wVecot: util.List[DenseDoubleVector],
               update_sigmoid_b: util.List[DenseDoubleVector],
               update_softmax_wVecot: util.List[DenseDoubleVector],
               update_softmax_b: util.List[DenseDoubleVector]
              ) = {
    for (i <- 0 until rank) {
      sigmoid_weight.increment(i, update_sigmoid_wVecot.get(i))
      softmax_weight.increment(i, update_softmax_wVecot.get(i))
      sigmoid_intercept.increment(i, update_sigmoid_b.get(i))
      softmax_intercept.increment(i, update_softmax_b.get(i))
    }

    sigmoid_weight.clock().get()
    softmax_weight.clock().get()
    sigmoid_intercept.clock().get()
    softmax_intercept.clock().get()
  }


}

class MLRPredictResult(id: Double, sig: Double) extends PredictResult {
  override def getText(): String = {
    (id + separator + format.format(sig))
  }
}
