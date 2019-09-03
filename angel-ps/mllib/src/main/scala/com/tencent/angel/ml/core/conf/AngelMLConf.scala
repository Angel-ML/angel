/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.core.conf

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.output.format.{ColIdValueTextRowFormat, RowIdColIdValueTextRowFormat, TextColumnFormat}

object AngelMLConf {
  val ANGEL_ML_TRAIN = "train"
  val ANGEL_ML_PREDICT = "predict"
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

  // Reg param
  val ML_REG_L2 = "ml.reg.l2"
  val DEFAULT_ML_REG_L2 = 0.0
  val ML_REG_L1 = "ml.reg.l1"
  val DEFAULT_ML_REG_L1 = 0.0

  // Embedding params
  val ML_FIELD_NUM = "ml.fm.field.num"
  val DEFAULT_ML_FIELD_NUM = -1
  val ML_RANK_NUM = "ml.fm.rank"
  val DEFAULT_ML_RANK_NUM = 8

  // Optimizer Params
  val DEFAULT_ML_OPTIMIZER = "Momentum"
  val ML_FCLAYER_OPTIMIZER = "ml.fclayer.optimizer"
  val DEFAULT_ML_FCLAYER_OPTIMIZER: String = DEFAULT_ML_OPTIMIZER
  val ML_EMBEDDING_OPTIMIZER = "ml.embedding.optimizer"
  val DEFAULT_ML_EMBEDDING_OPTIMIZER: String = DEFAULT_ML_OPTIMIZER
  val ML_INPUTLAYER_OPTIMIZER = "ml.inputlayer.optimizer"
  val DEFAULT_ML_INPUTLAYER_OPTIMIZER: String = DEFAULT_ML_OPTIMIZER

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

  // (MLP) Layer params
  val ML_NUM_CLASS = "ml.num.class"
  val DEFAULT_ML_NUM_CLASS = 2

  // RobustRegression params
  val ML_ROBUSTREGRESSION_LOSS_DELTA = "ml.robustregression.loss.delta"
  val DEFAULT_ML_ROBUSTREGRESSION_LOSS_DELTA = 1.0

  // Kmeans params
  val KMEANS_CENTER_NUM = "ml.kmeans.center.num"
  val DEFAULT_KMEANS_CENTER_NUM = 5
  val KMEANS_C = "ml.kmeans.c"
  val DEFAULT_KMEANS_C = 0.1

  // Decays
  val ML_OPT_DECAY_CLASS_NAME = "ml.opt.decay.class.name"
  val DEFAULT_ML_OPT_DECAY_CLASS_NAME = "StandardDecay"
  val ML_OPT_DECAY_ON_BATCH = "ml.opt.decay.on.batch"
  val DEFAULT_ML_OPT_DECAY_ON_BATCH = false
  val ML_OPT_DECAY_INTERVALS = "ml.opt.decay.intervals"
  val DEFAULT_ML_OPT_DECAY_INTERVALS = 100
  @Deprecated val ML_DECAY_INTERVALS = "ml.decay.intervals"
  @Deprecated val DEFAULT_ML_DECAY_INTERVALS = DEFAULT_ML_OPT_DECAY_INTERVALS
  val ML_OPT_DECAY_ALPHA = "ml.opt.decay.alpha"
  val DEFAULT_ML_OPT_DECAY_ALPHA = 0.001
  @Deprecated val ML_LEARN_DECAY = "ml.learn.decay"
  @Deprecated val DEFAULT_ML_LEARN_DECAY = 0.5
  val ML_OPT_DECAY_BETA = "ml.opt.decay.beta"
  val DEFAULT_ML_OPT_DECAY_BETA = 0.001

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

  val ML_NUM_TREE = "ml.number.tree"
  val DEFAULT_ML_NUM_TREE = 10

  // Tree Model Params
  val ML_TREE_TASK_TYPE = "ml.tree.task.type"
  val DEFAULT_ML_TREE_TASK_TYPE = "classification"
  val ML_TREE_MAX_DEPTH = "ml.tree.max.depth"
  val DEFAULT_ML_TREE_MAX_DEPTH = 2
  val ML_TREE_MAX_BIN = "ml.tree.max.bin"
  val DEFAULT_ML_TREE_MAX_BIN = 3
  val ML_TREE_SUB_SAMPLE_RATE = "ml.tree.sub.sample.rate"
  val DEFAULT_ML_TREE_SUB_SAMPLE_RATE = 1
  val ML_TREE_CATEGORICAL_FEATURE = "ml.tree.categorical.feature"
  val DEFAULT_ML_TREE_CATEGORICAL_FEATURE = ""
  val ML_TREE_FEATURE_SAMPLE_STRATEGY = "ml.tree.feature.sample.strategy"
  val DEFAULT_ML_TREE_FEATURE_SAMPLE_STRATEGY = "all"
  val ML_TREE_NODE_MIN_INSTANCE = "ml.tree.node.min.instance"
  val DEFAULT_ML_TREE_NODE_MIN_INSTANCE = 1
  val ML_TREE_NODE_MIN_INFOGAIN = "ml.tree.node.min.infogain"
  val DEFAULT_ML_TREE_NODE_MIN_INFOGAIN = 0

  val ML_TREE_IMPURITY = "ml.tree.impurity"
  val DEFAULT_ML_TREE_IMPURITY = "gini"
  val ML_TREE_AGGRE_MAX_MEMORY_MB = "ml.tree.aggr.max.memory.mb"
  val DEFAULT_ML_TREE_AGGRE_MAX_MEMORY_MB = 256

  val ML_GBDT_LOSS_FUNCTION: String = "ml.gbdt.loss.func"
  val DEFAULT_ML_GBDT_LOSS_FUNCTION: String = "binary:logistic"
  val ML_GBDT_EVAL_METRIC = "ml.gbdt.eval.metric"
  val DEFAULT_ML_GBDT_EVAL_METRIC = "error"
  val ML_GBDT_MULTI_CLASS_STRATEGY = "ml.gbdt.multi.class.strategy"
  val ML_GBDT_MULTI_CLASS_GRAD_CACHE = "ml.gbdt.multi.class.grad.cache"

  val ML_GBDT_FEATURE_SAMPLE_RATIO = "ml.gbdt.feature.sample.ratio"
  val DEFAULT_ML_GBDT_FEATURE_SAMPLE_RATIO = 1.0

  val ML_GBDT_HIST_SUBTRACTION = "ml.gbdt.hist.subtraction"
  val DEFAULT_ML_GBDT_HIST_SUBTRACTION = true
  val ML_GBDT_LIGHTER_CHILD_FIRST = "ml.gbdt.lighter.child.first"
  val DEFAULT_ML_GBDT_LIGHTER_CHILD_FIRST = true
  val ML_GBDT_FULL_HESSIAN = "ml.gbdt.full.hessian"
  val DEFAULT_ML_GBDT_FULL_HESSIAN = false

  val ML_GBDT_MIN_NODE_INSTANCE = "ml.gbdt.min.node.instance"
  val DEFAULT_ML_GBDT_MIN_NODE_INSTANCE = 1024
  val ML_GBDT_MIN_SPLIT_GAIN = "ml.gbdt.min.split.gain"
  val DEFAULT_ML_GBDT_MIN_SPLIT_GAIN = 0.0

  val ML_GBDT_MAX_LEAF_WEIGHT = "ml.gbdt.max.leaf.weight"
  val DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT = 0.0

  // AutoML params
  val ML_AUTO_TUNER_ITER = "ml.auto.tuner.iter"
  val DEFAULT_ML_AUTO_TUNER_ITER = 10
  val ML_AUTO_TUNER_MODEL = "ml.auto.tuner.model"
  val DEFAULT_ML_AUTO_TUNER_MODEL = "GaussianProcess"
  val ML_AUTO_TUNER_MINIMIZE = "ml.auto.tuner.minimize"
  val DEFAULT_ML_AUTO_TUNER_MINIMIZE = false
  val ML_AUTO_TUNER_PARAMS = "ml.auto.tuner.params"

}
