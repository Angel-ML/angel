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

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.lda.LDAModel._
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseIntVector, SparseIntVector}
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import com.tencent.angel.worker.predict.PredictResult
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

  // Matrix storing the values of Log Likelihood
  val LLH_MAT = "llh"

  // Number of vocabulary
  val WORD_NUM = "angel.lda.word.num";

  // Number of topic
  val TOPIC_NUM = "angel.lda.topic.num";

  // Number of documents
  val DOC_NUM = "angel.lda.doc.num"

  // Alpha value
  val ALPHA = "angel.lda.alpha";

  // Beta value
  val BETA = "angel.lda.beta";

  val PARALLEL_NUM = "angel.lda.worker.parallel.num";
  val SPLIT_NUM = "angel.lda.split.num"

}

class LDAModel(conf: Configuration, ctx: TaskContext) extends MLModel {

  def this(conf: Configuration) = {
    this(conf, null)
  }

  // Initializing parameters
  val V = conf.getInt(WORD_NUM, -1)
  val K = conf.getInt(TOPIC_NUM, -1)
  val M = conf.getInt(DOC_NUM, -1)
  val epoch = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  val alpha = conf.getFloat(ALPHA, 50.0F / K)
  val beta = conf.getFloat(BETA, 0.01F)
  val vbeta = beta * V
  val threadNum = conf.getInt(PARALLEL_NUM, 1)
  val splitNum = conf.getInt(SPLIT_NUM, 1)


  // Initializing model matrices
  val wtMat = new PSModel[DenseIntVector](ctx, WORD_TOPIC_MAT, V, K)
  wtMat.setRowType(RowType.T_INT_DENSE)
  wtMat.setOplogType("LIL_INT")

  val docTopic = new PSModel[SparseIntVector](ctx, DOC_NUM, M, K)
  docTopic.setRowType(RowType.T_INT_SPARSE)

  val tMat = new PSModel[DenseIntVector](ctx, TOPIC_MAT, 1, K)
  tMat.setRowType(RowType.T_INT_DENSE)
  tMat.setOplogType("DENSE_INT")

  // llh matrix is used to store the values of log likelihood
  val llh = new PSModel[DenseDoubleVector](ctx, LLH_MAT, 1, epoch + 1)
  llh.setRowType(RowType.T_DOUBLE_DENSE)
  llh.setOplogType("DENSE_DOUBLE")

  addPSModel(wtMat)
  addPSModel(tMat)
  addPSModel(llh)
  //setLoadPath(conf);
  setSavePath(conf)

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    // Save the word topic matrix to HDFS after Job is finished
    if (path != null)
      wtMat.setSavePath(path)
  }

  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    null
  }

  override def setLoadPath(conf: Configuration): Unit = ???

}
