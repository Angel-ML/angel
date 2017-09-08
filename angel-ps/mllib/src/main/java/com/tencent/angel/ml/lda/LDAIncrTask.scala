package com.tencent.angel.ml.lda

import com.tencent.angel.ml.lda.algo.CSRTokens
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.hadoop.io.{LongWritable, Text}

class LDAIncrTask(val ctx: TaskContext) extends BaseTask[LongWritable,
  Text, Text](ctx) {

  override def parse(key: LongWritable, value: Text): Text = {
    null
  }

  override def run(taskContext: TaskContext): Unit = {
    // load model
    val model = new LDAModel(conf, ctx)
    model.loadModel()

    // read data
    val data = CSRTokens.read(ctx, model.V, model.K)

    val infer = new Trainer(ctx, model, data)

  }

  override def preProcess(ctx: TaskContext): Unit = {

  }
}
