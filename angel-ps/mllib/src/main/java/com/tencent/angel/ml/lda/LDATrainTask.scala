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

import com.tencent.angel.ml.math.vector.DenseDoubleVector
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
      val doc  = parseDoc(text)
      if (doc != null) {
        doc.docId = did
        docs.add(doc)
        did += 1
        N += doc.len()
      }
    }

    // Initializing LDA model
    val lDAModel = new LDAModel(ctx.getConf, ctx)

    LOG.info(s"V=${lDAModel.V} K=${lDAModel.K} alpha=${lDAModel.alpha} beta=${lDAModel.beta} " +
      s"M=${docs.size()} tokens=$N")

    // Building the LDA learner
    val lDALearner = new LDALearner(ctx, docs, lDAModel)

    // Initilize topic assignment
    lDALearner.init()

    ctx.globalSync(lDAModel.tMat.getMatrixId)
    ctx.globalSync(lDAModel.wtMat.getMatrixId)

    val llh = lDALearner.loglikelihood()
    val update = new DenseDoubleVector(lDAModel.epoch + 1)
    update.set(0, llh)
    lDAModel.llh.increment(0, update)
    lDAModel.llh.clock().get()
    val globalLLh = lDAModel.llh.getRow(0).get(0)
    LOG.info(s"epoch=0 localllh=$llh globalllh=$globalLLh")

    ctx.incIteration()

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
