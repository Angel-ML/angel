package com.tencent.angel.ml.algorithm.classification.lr

import java.io.{BufferedWriter, DataOutputStream, OutputStreamWriter}
import java.text.DecimalFormat

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.classification.lr.LRModel._
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.utils.MathUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.TDoubleVector
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.{MemoryStorage, Storage}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.commons.logging.LogFactory

object LRModel {
  val LR_WEIGHT_MAT = "lr_weight"
}

class LRModel(ctx: TaskContext, conf: Configuration) extends AlgorithmModel {
  private val LOG = LogFactory.getLog(classOf[LRModel])

  def this(conf: Configuration) = {
    this(null, conf)
  }

  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, 10000)
  val weight = new PSModel[TDoubleVector](ctx, LR_WEIGHT_MAT, 1, feaNum)

  addPSModel(LR_WEIGHT_MAT, weight)

  var localWeight: TDoubleVector = _

  def pullWeightFromPS() = {
    localWeight = weight.getRow(0)
//    localWeight = weight.pullVectorFromPS(ctx, 0)
  }

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    weight.setSavePath(path)
  }

  override def setLoadPath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_LOAD_MODEL_PATH)
    weight.setLoadPath(path)
  }

  override
  def predict(dataSet: Storage[LabeledData]): Storage[PredictResult] = {
    val predict = new MemoryStorage[PredictResult](-1)

    dataSet.resetReadIndex()
    for (idx: Int <- 0 until dataSet.getTotalElemNum) {
      val instance = dataSet.read
      val id = instance.getY
      val dot = localWeight.dot(instance.getX)
      val sig = MathUtils.sigmoid(dot)
      predict.put(new lrPredictResult(id, dot, sig))
    }
    predict
  }
}

class lrPredictResult(id: Double, dot: Double, sig: Double) extends PredictResult {
  val format = new DecimalFormat("0.000000");

  override
  def writeText(output: DataOutputStream): Unit = {

    output.writeBytes(id + PredictResult.separator + format.format(dot) +
      PredictResult.separator + format.format(sig))
  }

}

