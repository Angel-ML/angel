package com.tencent.angel.ml.modelparser

import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}


class ModelParserTask(val ctx: TaskContext) extends BaseTask[LongWritable, Text, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[ModelParserTask])

  override
  def parse(key: LongWritable, value: Text): Text = {
    // Do nothing
    null
  }

  override
  def preProcess(ctx: TaskContext) {
    // Do nothing in the preprocess function
  }

  override def run(ctx: TaskContext): Unit = {
    val start = System.currentTimeMillis()
    val parser = new ModelParser(ctx.getConf)
    parser.parse()
    val end = System.currentTimeMillis()

  }
}
