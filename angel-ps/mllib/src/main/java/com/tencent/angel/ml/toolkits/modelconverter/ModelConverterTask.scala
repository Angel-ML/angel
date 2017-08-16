package com.tencent.angel.ml.toolkits.modelconverter

import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

/*
 * ModelConverterTask convert the binary PSModel output files to plain text
 */
class ModelConverterTask(val ctx: TaskContext) extends BaseTask[LongWritable, Text, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[ModelConverterTask])

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
    val parser = new ModelConverter(ctx.getConf)
    parser.parse()
  }
}
