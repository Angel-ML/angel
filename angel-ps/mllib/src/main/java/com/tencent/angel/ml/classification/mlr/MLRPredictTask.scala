package com.tencent.angel.ml.classification.mlr

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.task.{PredictTask, TaskContext}
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Created by hbghh on 2017/8/17.
  */
class MLRPredictTask(ctx: TaskContext) extends PredictTask[LongWritable, Text](ctx) {
  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  val dataFormat = conf.get(MLConf.ML_DATAFORMAT)

  def predict(ctx: TaskContext) {
    predict(ctx, MLRModel(ctx, conf), trainDataBlock);
  }

  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, dataFormat, false)
  }


}
