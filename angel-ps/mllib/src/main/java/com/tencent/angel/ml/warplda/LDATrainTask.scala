package com.tencent.angel.ml.warplda

import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 9/1/17.
  */
class LDATrainTask(val ctx: TaskContext) extends BaseTask[LongWritable, Text, Text](ctx) {

  private val LOG = LogFactory.getLog(classOf[LDATrainTask])

  override
  def parse(key: LongWritable, value: Text): Text = {
    // Do nothing
    null
  }

  override
  def preProcess(ctx: TaskContext) {
    // Do nothing in the preprocess function
  }

  @throws[Exception]
  def run(ctx: TaskContext): Unit = {
    // Read documents
    val reader = ctx.getReader[LongWritable, Text]
    var docs   = new ArrayBuffer[Document]()
    var did = 0
    var N = 0
    while (reader.nextKeyValue()) {
      val text = reader.getCurrentValue
      val doc  = new Document(text.toString)
      if (doc != null) {
        docs.+=(doc)
        did += 1
        N += doc.len
      }
    }
    reader.close()

    // Initializing LDA model
    val model = new LDAModel(ctx.getConf, ctx)

    LOG.info(s"V=${model.V} K=${model.K} alpha=${model.alpha} "
      + s"beta=${model.beta} M=${docs.length} tokens=$N "
      + s"threadNum=${model.threadNum} mh=${model.mh}")

    // build topic structures
    val data = new WTokens(model.V, docs.length)
    data.build(docs, model.K,model.mh)
    docs.clear()

    // training
    val learner = new LDATrainer(ctx, model, data)
    learner.initialize()
    learner.train(model.epoch)

    // save values
    if (model.saveWordTopic) learner.saveWordTopic(model)
    if (model.saveDocTopic) learner.saveDocTopic(data, model)
  }

}