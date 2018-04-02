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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.warplda

import com.tencent.angel.conf.AngelConf._
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.warplda.LDAModel._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

/**
  * The parameters of LDA model.
  */
object LDAModel {
  // Word topic matrix for Phi
  val WORD_TOPIC_MAT = "word_topic"

  // Doc topic count matrix for Theta
  val DOC_TOPIC_MAT = "doc_topic"

  // Topic count matrix
  val TOPIC_MAT = "topic"

  // Number of vocabulary
  val WORD_NUM = "ml.lda.word.num";

  // Number of topic
  val TOPIC_NUM = "ml.lda.topic.num";

  // Number of documents
  val DOC_NUM = "ml.lda.doc.num"

  // Alpha value
  val ALPHA = "ml.lda.alpha";

  // Beta value
  val BETA = "ml.lda.beta";
  val MH = "ml.lda.mh";

  val SPLIT_NUM = "ml.lda.split.num"

  val SAVE_DOC_TOPIC = "save.doc.topic"
  val SAVE_WORD_TOPIC = "save.word.topic"

}

class LDAModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  // Initializing parameters
  val V = conf.getInt(WORD_NUM, 1)
  val K = conf.getInt(TOPIC_NUM, 1)
  val M = conf.getInt(DOC_NUM, 1)
  val epoch = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  val alpha = conf.getFloat(ALPHA, 50.0F / K)
  val beta = conf.getFloat(BETA, 0.01F)
  val vBeta = beta * V

  val threadNum = conf.getInt(ANGEL_WORKER_THREAD_NUM, DEFAULT_ANGEL_WORKER_THREAD_NUM)
  val splitNum = conf.getInt(SPLIT_NUM, 1)
  val psNum = conf.getInt(ANGEL_PS_NUMBER, 1)
  val parts = conf.getInt(ML_MODEL_PART_PER_SERVER, DEFAULT_ML_MODEL_PART_PER_SERVER)


  val saveDocTopic = conf.getBoolean(SAVE_DOC_TOPIC, false)
  val saveWordTopic = conf.getBoolean(SAVE_WORD_TOPIC, true)

  val mh = conf.getInt(MH, 10)

  // Initializing model matrices

  val wtMat = PSModel(WORD_TOPIC_MAT, V, K, Math.max(1, V / psNum), K)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("SPARSE_INT")

  val tMat = PSModel(TOPIC_MAT, 1, K, 1, K)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)


  addPSModel(wtMat)
  addPSModel(tMat)


  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    null
  }

}
