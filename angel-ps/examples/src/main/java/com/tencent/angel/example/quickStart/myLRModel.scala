package com.tencent.angel.example.quickStart

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

object myLRModel{
  def apply(ctx:TaskContext, conf: Configuration) = {
    new myLRModel(ctx, conf)
  }
}



class myLRModel(_ctx: TaskContext, conf: Configuration) extends MLModel(_ctx){
  val N = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)

  val weight = PSModel[DenseDoubleVector]("mylr.weight", 1, N)
  weight.setAverage(true)
  addPSModel(weight)

  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???

  override def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
    if (path != null)
      weight.setSavePath(path)
  }

  override def setLoadPath(conf: Configuration): Unit = ???
}
