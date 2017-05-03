package com.tencent.angel.ml.algorithm.lda

import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.lda.LDAModel._
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, DenseIntVector, SparseIntVector}
import com.tencent.angel.ml.model.{AlgorithmModel, PSModel}
import com.tencent.angel.protobuf.generated.MLProtos.RowType
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.Storage
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

class LDAModel(conf: Configuration, ctx: TaskContext) extends AlgorithmModel {

  def this(conf: Configuration) = {
    this(conf, null)
  }

  // Initializing parameters
  val V = conf.getInt(WORD_NUM, -1)
  val K = conf.getInt(TOPIC_NUM, -1)
  val M = conf.getInt(DOC_NUM, -1)
  val epoch = conf.getInt(MLConf.ML_EPOCH_NUM, 10)
  val alpha = conf.getFloat(ALPHA, 50.0F / K)
  val beta  = conf.getFloat(BETA, 0.01F)
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
  val llh = new PSModel[DenseDoubleVector](ctx, LLH_MAT, 1, epoch)
  llh.setRowType(RowType.T_DOUBLE_DENSE)
  llh.setOplogType("DENSE_DOUBLE")

  addPSModel(wtMat)
  addPSModel(tMat)
  addPSModel(llh)

  override
  def setSavePath(conf: Configuration): Unit = {
    val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)

    // Save the word topic matrix to HDFS after Job is finished
    wtMat.setSavePath(path)
  }

  override
  def predict(dataSet: Storage[LabeledData]): Storage[PredictResult] = {
    null
  }
}
