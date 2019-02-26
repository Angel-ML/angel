package com.tencent.angel.ml.core.conf

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.output.format.{ColIdValueTextRowFormat, RowIdColIdValueTextRowFormat, TextColumnFormat}

object AngelMLConf {
  val ANGEL_ML_TRAIN = "train"
  val ANGEL_ML_PREDICT = "com/tencent/angel/ml/predict"
  val ANGEL_ML_INC_TRAIN = "inctrain"

  // Data params
  val ML_DATA_INPUT_FORMAT = "ml.data.type"
  val DEFAULT_ML_DATA_INPUT_FORMAT = "libsvm"
  val ML_DATA_SPLITOR = "ml.data.splitor"
  val DEFAULT_ML_DATA_SPLITOR = "\\s+"
  val ML_DATA_HAS_LABEL = "ml.data.has.label"
  val DEFAULT_ML_DATA_HAS_LABEL = true
  val ML_DATA_LABEL_TRANS = "ml.data.label.trans.class"
  val DEFAULT_ML_DATA_LABEL_TRANS = "NoTrans"
  val ML_DATA_LABEL_TRANS_THRESHOLD = "ml.data.label.trans.threshold"
  val DEFAULT_ML_DATA_LABEL_TRANS_THRESHOLD = 0
  val ML_VALIDATE_RATIO = "ml.data.validate.ratio"
  val DEFAULT_ML_VALIDATE_RATIO = 0.05
  val ML_FEATURE_INDEX_RANGE = "ml.feature.index.range"
  val DEFAULT_ML_FEATURE_INDEX_RANGE = -1
  val ML_BLOCK_SIZE = "ml.block.size"
  val DEFAULT_ML_BLOCK_SIZE = 1000000
  val ML_DATA_USE_SHUFFLE = "ml.data.use.shuffle"
  val DEFAULT_ML_DATA_USE_SHUFFLE = false
  val ML_DATA_POSNEG_RATIO = "ml.data.posneg.ratio"
  val DEFAULT_ML_DATA_POSNEG_RATIO = -1


  // Worker params
  val ANGEL_WORKER_THREAD_NUM = "angel.worker.thread.num"
  val DEFAULT_ANGEL_WORKER_THREAD_NUM = 1

  // network param
  val ANGEL_COMPRESS_BYTES = "angel.compress.bytes"
  val DEFAULT_ANGEL_COMPRESS_BYTES = 8

  // Model params
  val ML_MODEL_CLASS_NAME = "ml.model.class.name"
  val DEFAULT_ML_MODEL_CLASS_NAME = ""
  val ML_MODEL_SIZE = "ml.model.size"
  val DEFAULT_ML_MODEL_SIZE = -1
  val ML_MODEL_TYPE = "ml.model.type"
  val DEFAULT_ML_MODEL_TYPE = RowType.T_FLOAT_DENSE.toString
  val ML_MODEL_IS_CLASSIFICATION = "ml.model.is.classification"
  val DEFAULT_ML_MODEL_IS_CLASSIFICATION = true

  val ML_EPOCH_NUM = "ml.epoch.num"
  val DEFAULT_ML_EPOCH_NUM = 30
  val ML_BATCH_SAMPLE_RATIO = "ml.batch.sample.ratio"
  val DEFAULT_ML_BATCH_SAMPLE_RATIO = 1.0
  val ML_LEARN_RATE = "ml.learn.rate"
  val DEFAULT_ML_LEARN_RATE = 0.5

  val ML_NUM_UPDATE_PER_EPOCH = "ml.num.update.per.epoch"
  val DEFAULT_ML_NUM_UPDATE_PER_EPOCH = 10

  val ML_MINIBATCH_SIZE = "ml.minibatch.size"
  val DEFAULT_ML_MINIBATCH_SIZE = 128

  val ML_VERABLE_SAVE_WITHSLOT = "ml.variable.save.withslot"
  val DEFAULT_ML_VERABLE_SAVE_WITHSLOT = false

  val ML_LOSSFUNCTION_HUBER_DELTA = "ml.lossfunction.huber.delta"
  val DEFAULT_ML_LOSSFUNCTION_HUBER_DELTA = 1.0

  // MLR parameters
  val ML_MLR_RANK = "ml.mlr.rank"
  val DEFAULT_ML_MLR_RANK = 5

  // RobustRegression params
  val ML_ROBUSTREGRESSION_LOSS_DELTA = "ml.robustregression.loss.delta"
  val DEFAULT_ML_ROBUSTREGRESSION_LOSS_DELTA = 1.0

  // Kmeans params
  val KMEANS_CENTER_NUM = "ml.kmeans.center.num"
  val DEFAULT_KMEANS_CENTER_NUM = 5
  val KMEANS_C = "ml.kmeans.c"
  val DEFAULT_KMEANS_C = 0.1

  // GBDT Params
  val ML_GBDT_TASK_TYPE = "ml.gbdt.task.type"
  val DEFAULT_ML_GBDT_TASK_TYPE = "classification"
  val ML_GBDT_CLASS_NUM = "ml.gbdt.class.num"
  val DEFAULT_ML_GBDT_CLASS_NUM = 2
  val ML_GBDT_PARALLEL_MODE = "ml.gbdt.parallel.mode"
  val DEFAULT_ML_GBDT_PARALLEL_MODE = "data"
  val ML_GBDT_TREE_NUM = "ml.gbdt.tree.num"
  val DEFAULT_ML_GBDT_TREE_NUM = 10
  val ML_GBDT_TREE_DEPTH = "ml.gbdt.tree.depth"
  val DEFAULT_ML_GBDT_TREE_DEPTH = 5
  val ML_GBDT_MAX_NODE_NUM = "ml.gbdt.max.node.num"
  val ML_GBDT_SPLIT_NUM = "ml.gbdt.split.num"
  val DEFAULT_ML_GBDT_SPLIT_NUM = 5
  val ML_GBDT_ROW_SAMPLE_RATIO = "ml.gbdt.row.sample.ratio"
  val DEFAULT_ML_GBDT_ROW_SAMPLE_RATIO = 1
  val ML_GBDT_SAMPLE_RATIO = "ml.gbdt.sample.ratio"
  val DEFAULT_ML_GBDT_SAMPLE_RATIO = 1
  val ML_GBDT_MIN_CHILD_WEIGHT = "ml.gbdt.min.child.weight"
  val DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT = 0.01
  val ML_GBDT_REG_ALPHA = "ml.gbdt.reg.alpha"
  val DEFAULT_ML_GBDT_REG_ALPHA = 0
  val ML_GBDT_REG_LAMBDA = "ml.gbdt.reg.lambda"
  val DEFAULT_ML_GBDT_REG_LAMBDA = 1.0
  val ML_GBDT_THREAD_NUM = "ml.gbdt.thread.num"
  val DEFAULT_ML_GBDT_THREAD_NUM = 20
  val ML_GBDT_BATCH_SIZE = "ml.gbdt.batch.size"
  val DEFAULT_ML_GBDT_BATCH_SIZE = 10000
  val ML_GBDT_SERVER_SPLIT = "ml.gbdt.server.split"
  val DEFAULT_ML_GBDT_SERVER_SPLIT = false
  val ML_GBDT_CATE_FEAT = "ml.gbdt.cate.feat"
  val DEFAULT_ML_GBDT_CATE_FEAT = "none"

  /** The loss sum of all samples */
  val TRAIN_LOSS = "train.loss"
  val VALID_LOSS = "validate.loss"
  val LOG_LIKELIHOOD = "log.likelihood"

  /** The predict error of all samples */
  val TRAIN_ERROR = "train.error"
  val VALID_ERROR = "validate.error"

  /** The predict error of all samples */
  val ML_MATRIX_DOT_USE_PARALLEL_EXECUTOR = "ml.matrix.dot.use.parallel.executor"
  val DEFAULT_ML_MATRIX_DOT_USE_PARALLEL_EXECUTOR = false
}
