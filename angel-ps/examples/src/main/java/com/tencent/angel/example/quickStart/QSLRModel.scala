package com.tencent.angel.example.quickstart

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

class QSLRModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx){
  val N = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)

  val weight = PSModel[DenseDoubleVector]("qs.lr.weight", 1, N)
               .setAverage(true)
  addPSModel(weight)

  setSavePath(conf)
  setLoadPath(conf)

  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = ???
}
