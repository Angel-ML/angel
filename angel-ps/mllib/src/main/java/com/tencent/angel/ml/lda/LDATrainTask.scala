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

import java.util

import com.tencent.angel.ml.lda.algo.{CSRTokens, Document}
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
    var N = 0
    while (reader.nextKeyValue()) {
      val text = reader.getCurrentValue
      val doc  = new Document(text.toString)
      if (doc != null) {
        docs.add(doc)
        did += 1
        N += doc.len()
      }
    }

    // Initializing LDA model
    val model = new LDAModel(ctx.getConf, ctx)

    LOG.info(s"V=${model.V} K=${model.K} alpha=${model.alpha} "
       + s"beta=${model.beta} M=${docs.size()} tokens=$N "
       + s"threadNum=${model.threadNum}")

    // build topic structures
    val data = new CSRTokens(model.V, docs.size())
    data.build(docs, model.K)
    docs.clear()

    // training
    val learner = new Trainer(ctx, model, data)
    learner.initialize()
    learner.train(model.epoch)

    // save values
    if (model.saveWordTopic) learner.saveWordTopic(model)
    if (model.saveDocTopic) learner.saveDocTopic(data, model)
  }

}
