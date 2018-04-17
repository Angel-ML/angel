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

import java.io.{BufferedReader, InputStreamReader}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.conf.AngelConf._
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.lda.LDAModel._
import com.tencent.angel.ml.math.vector.DenseIntVector
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.ml.utils.HDFSUtils
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer


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

  // Vocabulary ID matrix
  val VOCABULARY_MAT = "vocabulary"

  // Number of vocabulary
  val WORD_NUM = "ml.lda.word.num"

  // Number of topic
  val TOPIC_NUM = "ml.lda.topic.num"

  // Number of documents
  val DOC_NUM = "ml.lda.doc.num"

  val TOKEN_NUM = "ml.lda.token.num"

  // Alpha value
  val ALPHA = "ml.lda.alpha"

  // Beta value
  val BETA = "ml.lda.beta"

  // Whether to save doc.topic matrix
  val SAVE_DOC_TOPIC = "save.doc.topic"

  val SAVE_DOC_TOPIC_DISTRIBUTION = "save.doc.topic.distribution"

  // Whether to save word.topic matrix
  val SAVE_WORD_TOPIC = "save.word.topic"

  val SAVE_TOPIC_WORD_DISTRIBUTION = "save.topic.word.distribution"

  val WORD_NUM_PATH = "word.num.path"

  val SAVE_PATH = "save.path"
}

class LDAModel(conf: Configuration, _ctx: TaskContext = null) extends MLModel(conf, _ctx) {

  val LOG = LogFactory.getLog(classOf[LDAModel])

  val numTasks = conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, -1)

  // Initializing parameters
//  var V: Int = 0
//  val path = conf.get(WORD_NUM_PATH)
//  if (path != null && path.length > 0)
//    V = HDFSUtils.readFeatureNum(path, conf)
//  else
//    V = conf.getInt(WORD_NUM, 1)


  val K = conf.getInt(TOPIC_NUM, 1)
  val M = conf.getInt(DOC_NUM, 1)
  val epoch = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  val alpha = conf.getFloat(ALPHA, 50.0F / K)
  val beta = conf.getFloat(BETA, 0.01F)
  var vBeta = 0F
  var V = conf.getInt(WORD_NUM, 1)

  val threadNum = conf.getInt(ANGEL_WORKER_THREAD_NUM, 4)

  val psNum = conf.getInt(ANGEL_PS_NUMBER, 1)

  val saveDocTopic = conf.getBoolean(SAVE_DOC_TOPIC, false)
  val saveWordTopic = conf.getBoolean(SAVE_WORD_TOPIC, true)
  val saveDocTopicDistribution = conf.getBoolean(SAVE_DOC_TOPIC_DISTRIBUTION, false)
  val saveTopicWordDistribution = conf.getBoolean(SAVE_TOPIC_WORD_DISTRIBUTION, false)

  // Initializing model matrices

  var wtMat: PSModel = PSModel(WORD_TOPIC_MAT, V, K, blockNum(V, K), K)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("SPARSE_INT")

  val tMat = PSModel(TOPIC_MAT, 1, K, 1, K)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)


  val vocabularyMatrix = PSModel(VOCABULARY_MAT, 1, numTasks, 1, numTasks)
    .setRowType(RowType.T_INT_DENSE)
    .setOplogType("DENSE_INT")
    .setNeedSave(false)


  val actType = conf.get(AngelConf.ANGEL_ACTION_TYPE)
  actType match {
    case MLConf.ANGEL_ML_PREDICT => addPSModel(wtMat)
    case _ =>
  }
  addPSModel(tMat)
  addPSModel(vocabularyMatrix)

  //  setSavePath(conf)
  //  setLoadPath(conf)

  override
  def predict(dataSet: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    null
  }

  def blockNum(V: Int, K: Int): Int = {
    val blockNum = 20 * 1024 * 1024 / (K * 4)
    return Math.max(1, Math.max(V / 10000, Math.min(V / psNum, blockNum)))
  }


  def loadModel(): Unit = {
    val paths = getPaths()
    val update = new DenseIntVector(K)

    System.out.println(paths.length)

    for (i <- 0 until paths.length) {
      val path = paths(i)
      LOG.info(s"Load model from path ${path}")
      val fs = path.getFileSystem(conf)

      val in = new BufferedReader(new InputStreamReader(fs.open(path)))

      var finish = false
      while (!finish) {
        in.readLine() match {
          case line: String =>
            val parts = line.split(" ")
            val topic = parts(0).toInt
            val vector = new DenseIntVector(K)
            for (i <- 1 until parts.length) {
              val pp = parts(i).split(":")
              vector.set(pp(0).toInt, pp(1).toInt)
              update.plusBy(pp(0).toInt, pp(1).toInt)
            }
            wtMat.increment(topic, vector)
          case null => finish = true
        }
      }

      in.close()
    }

    tMat.increment(0, update)
    wtMat.clock().get()
    tMat.clock().get()
  }

  def getPaths(): Array[Path] = {
    val taskId = ctx.getTaskIndex
    val total = ctx.getTotalTaskNum
    val dir = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    val base = dir + "/" + "word_topic"

    val basePath = new Path(base)
    val fs = basePath.getFileSystem(conf)
    if (!fs.exists(basePath))
      throw new AngelException(s"Model load path does not exist ${base}")

    if (!fs.isDirectory(basePath))
      throw new AngelException(s"Model load path ${base} is not a directory")

    val statuses = fs.listStatus(basePath)
    val ret = new ArrayBuffer[Path]()
    for (i <- 0 until statuses.length) {
      val status = statuses(i)
      if (status.getPath != null && i % total == taskId)
        ret.append(status.getPath)
    }

    ret.toArray
  }

}
