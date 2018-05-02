/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.lda

import com.tencent.angel.ml.lda.algo.{CSRTokens, Document}
import com.tencent.angel.ml.math.vector.{DenseIntVector, SparseIntVector}
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.{BaseTask, TaskContext}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Latent Dirichlet Allocation (LDA) is a probabilic generative model.
  */
class LDATrainTask(val ctx: TaskContext) extends BaseTask[LongWritable, Text, Document](ctx) {

  private val LOG = LogFactory.getLog(classOf[LDATrainTask])

  var docs = new MemoryDataBlock[Document](-1)
  var N = 0
  var did = 0

  override
  def parse(key: LongWritable, value: Text): Document = {
    val doc = new Document(value.toString)
    if (doc != null) {
      did += 1
      N += doc.len()
    }
    doc
  }

  override
  def preProcess(ctx: TaskContext) {
    val reader = ctx.getReader[LongWritable, Text]
    while (reader.nextKeyValue()) {
      docs.put(parse(reader.getCurrentKey(), reader.getCurrentValue))
    }
  }

  def calcuateVocabularyNum(model: LDAModel): Int = {

    val iter = docs.getvList().iterator()
    var maxVocabularyId = -1
    while (iter.hasNext) {
      val doc = iter.next()
      maxVocabularyId = Math.max(maxVocabularyId, doc.wids.max)
    }

    val taskId = ctx.getTaskIndex
    val taskNum = ctx.getTotalTaskNum
    val update = new SparseIntVector(taskNum)
    update.set(taskId, maxVocabularyId)
    model.vocabularyMatrix.increment(0, update)
    model.vocabularyMatrix.clock()

    val values = model.vocabularyMatrix.getRow(0).asInstanceOf[DenseIntVector].getValues

    values.max + 1
  }

  @throws[Exception]
  def run(ctx: TaskContext): Unit = {
    // Initializing LDA model
    val model = new LDAModel(ctx.getConf, ctx)

    val numVocabulary = calcuateVocabularyNum(model)

    LOG.info(s"V=${model.V} K=${model.K} alpha=${model.alpha} "
      + s"beta=${model.beta} M=${docs.size()} tokens=$N "
      + s"threadNum=${model.threadNum}")

    LOG.info(s"V=${model.V} real V=${numVocabulary}")

    model.V = numVocabulary
    model.vBeta = model.beta * model.V

    val taskId = ctx.getTaskIndex
    model.wtMat.matrixCtx.setRowNum(numVocabulary)
    model.wtMat.matrixCtx.setMaxRowNumInBlock(model.blockNum(numVocabulary, model.K))
    model.wtMat.matrixCtx.setMaxColNumInBlock(model.K)

    if (taskId == 0) {
      ctx.createMatrix(model.wtMat.matrixCtx, 10000)
    }

    // build topic structures
    val tokens = new CSRTokens(model.V, docs.size())
    tokens.build(docs, model.K)
    docs.clean()
    LOG.info(s"build data")

    // synchronize here, guaranteeing that word.topic matrix has been built

    model.tMat.clock(false).get()
    model.tMat.getRow(0)


    // training
    val learner = new LDALearner(ctx, model, tokens)
    learner.initialize()
    learner.train(model.epoch)

    // save values
    if (model.saveWordTopic) learner.saveWordTopic(model)
    if (model.saveDocTopic) learner.saveDocTopic(tokens, model)
    if (model.saveDocTopicDistribution) learner.saveDocTopicDistribution(tokens, model)
    if (model.saveTopicWordDistribution) learner.saveWordTopicDistribution(model)
  }

}
