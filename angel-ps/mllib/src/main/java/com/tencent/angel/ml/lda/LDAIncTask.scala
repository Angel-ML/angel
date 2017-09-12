package com.tencent.angel.ml.lda

import com.tencent.angel.ml.lda.algo.{Document, CSRTokens}
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.hadoop.io.{LongWritable, Text}

class LDAIncTask(val ctx: TaskContext) extends BaseTask[LongWritable, Text, Document](ctx) {

  var did = 0
  var N = 0
  var docs = new MemoryDataBlock[Document](-1)

  override
  def parse(key: LongWritable, value: Text): Document = {
    val doc  = new Document(value.toString)
    if (doc != null) {
      did += 1
      N += doc.len()
    }
    doc
  }

  override def run(taskContext: TaskContext): Unit = {
    // load model
    val model = new LDAModel(conf, ctx)
    model.loadModel()

    val data = new CSRTokens(model.V, docs.size())
    data.build(docs, model.K)
    docs.clean()

    val infer = new LDALearner(ctx, model, data)
    //infer.train()

  }
  override
  def preProcess(ctx: TaskContext) {
    val reader = ctx.getReader[LongWritable, Text]
    while (reader.nextKeyValue()) {
      val doc  = new Document(reader.getCurrentValue.toString)
      docs.put(doc)
    }
  }
}
