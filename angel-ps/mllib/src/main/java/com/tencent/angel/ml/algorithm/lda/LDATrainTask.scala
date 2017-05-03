package com.tencent.angel.ml.algorithm.lda

import java.util

import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Latent Dirichlet Allocation (LDA) is a probabilic generative model.
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
    val docs   = new util.ArrayList[Document]()
    var did = 0
    while (reader.nextKeyValue()) {
      val text = reader.getCurrentValue
      val doc  = parseDoc(text)
      if (doc != null) {
        doc.docId = did
        docs.add(doc)
        did += 1
      }
    }

    // Initializing LDA model
    val lDAModel = new LDAModel(ctx.getConf, ctx)

    // Building the LDA learner
    val lDALearner = new LDALearner(ctx, docs, lDAModel)

    // Initilize topic assignment
    lDALearner.init()
    lDAModel.wtMat.clock(false)
    lDAModel.tMat.clock(false)
    ctx.increaseIteration()

    // Waiting for all workers finishing initializing
    ctx.globalSync()

    LOG.info("After initializing llh " + lDALearner.loglikelihood())

    // Training
    for (i <- 1 to lDAModel.epoch)
      lDALearner.trainOneEpoch(i)

  }

  def parseDoc(text: Text): Document = {
    val str = text.toString.trim()

    if (str.length == 0)
      return null

    val splits = str.split(" ")
    if (splits.length < 1)
      return null

    val wids = new Array[Int](splits.length)
    for (i <- 0 until splits.length)
      wids(i) = splits(i).toInt

    new Document(-1, wids)
  }



}
