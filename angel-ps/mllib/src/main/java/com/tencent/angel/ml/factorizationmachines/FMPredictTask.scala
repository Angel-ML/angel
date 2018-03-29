package com.tencent.angel.ml.factorizationmachines


import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.PredictTask
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.io.{LongWritable, Text}

class FMPredictTask(val ctx: TaskContext) extends PredictTask[LongWritable, Text](ctx) {
  override def predict(taskContext: TaskContext): Unit = {
    predict(ctx, FMModel(ctx.getConf, taskContext), taskDataBlock)
  }

  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }
}
