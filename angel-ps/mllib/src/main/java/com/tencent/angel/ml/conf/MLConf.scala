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

package com.tencent.angel.ml.conf

import com.tencent.angel.ml.matrix.RowType

object MLConf {

  // The Train action means to learn a model while Predict action uses the model to predict beyond unobserved samples.
  val ANGEL_ML_TRAIN = "train"
  val ANGEL_ML_PREDICT = "predict"
  val ANGEL_ML_INC_TRAIN = "inctrain"

  // Data params
  val ML_DATA_INPUT_FORMAT = "ml.data.input.type"
  val DEFAULT_ML_DATA_FORMAT = "dummy"
  val ML_DATA_SPLITOR = "ml.data.splitor"
  val DEFAULT_ML_DATA_SPLITOR = " "
  val ML_DATA_IS_NEGY = "ml.data.is.negy"
  val DEFAULT_ML_DATA_IS_NEGY = true
  val ML_DATA_HAS_LABEL = "ml.data.has.label"
  val DEFAULT_ML_DATA_HAS_LABEL = true
  val ML_DATA_IS_CLASSIFICATION = "ml.data.is.classification"
  val DEFAULT_ML_DATA_IS_CLASSIFICATION = true
  val ML_VALIDATE_RATIO = "ml.data.validate.ratio"
  val DEFAULT_ML_VALIDATE_RATIO = 0.05
  val ML_FEATURE_INDEX_RANGE = "ml.feature.index.range"
  val DEFAULT_ML_FEATURE_INDEX_RANGE = -1
  val ML_PULL_WITH_INDEX_ENABLE = "ml.pull.with.index.enable"
  val DEFAULT_ML_PULL_WITH_INDEX_ENABLE = false


  // Worker params
  val ANGEL_WORKER_THREAD_NUM = "angel.worker.thread.num"
  val DEFAULT_ANGEL_WORKER_THREAD_NUM = 1

  // network param
  val ANGEL_COMPRESS_BYTES = "angel.compress.bytes"
  val DEFAULT_ANGEL_COMPRESS_BYTES = 8

  // Model params
  val ML_MODEL_SIZE = "ml.model.size"
  val DEFAULT_ML_MODEL_SIZE = -1
  val ML_MODEL_TYPE = "ml.model.type"
  val DEFAULT_ML_MODEL_TYPE = RowType.T_DOUBLE_DENSE.toString
  val ML_MODEL_PART_PER_SERVER = "ml.model.part.per.server"
  val DEFAULT_ML_MODEL_PART_PER_SERVER = 5
  val ML_MODEL_INPUT_PATH = "ml.model.input.path"
  val ML_MODEL_OUTPUT_PATH = "ml.model.output.path"
  val ML_MODEL_CONVERT_THREAD_COUNT = "ml.model.convert.thread.count"
  val DEFAULT_ML_MODEL_CONVERT_THREAD_COUNT = 1
  val ML_MODEL_NAME = "ml.model.name"

  // mini-batch sgd params
  val ML_OPT_METHOD = "ml.opt.method"
  val DEFAULT_ML_OPT_METHOD = "MiniBatchSGD"
  val ML_EPOCH_NUM = "ml.epoch.num"
  val DEFAULT_ML_EPOCH_NUM = 10
  val ML_BATCH_SAMPLE_RATIO = "ml.batch.sample.ratio"
  val DEFAULT_ML_BATCH_SAMPLE_RATIO = 1.0
  val ML_LEARN_RATE = "ml.learn.rate"
  val DEFAULT_ML_LEAR_RATE = 1.0
  val ML_LEARN_DECAY = "ml.learn.decay"
  val DEFAULT_ML_LEARN_DECAY = 0.5
  val ML_NUM_UPDATE_PER_EPOCH = "ml.num.update.per.epoch"
  val DEFAULT_ML_NUM_UPDATE_PER_EPOCH = 10
  val ML_POSITIVE_SAMPLE_WEIGHT = "ml.positive.sample.weight"
  val DEFAULT_ML_POSITIVE_SAMPLE_WEIGHT = 1.0
  val ML_NEGATIVE_SAMPLE_WEIGHT = "ml.negative.sample.weight"
  val DEFAULT_ML_NEGATIVE_SAMPLE_WEIGHT = 1.0
  val ML_MINIBATCH_SIZE = "ml.minibatch.size"
  val DEFAULT_ML_MINIBATCH_SIZE = 128
  val ML_LEARN_RATE_BOUND = "ml.learn.rate.bound"
  val DEFAULT_ML_LEARN_RATE_BOUND = 5

  // Adamm params
  val ML_OPT_ADAMM_RHO = "ml.opt.adamm.rho"
  val DEFAULT_ML_OPT_ADAMM_RHO = 0.9
  val ML_OPT_ADAMM_PHI = "ml.opt.adamm.phi"
  val DEFAULT_ML_OPT_ADAMM_PHI = 0.99

  // Momentum params
  val ML_OPT_MOMENTUM_RHO = "ml.opt.momentum.rho"
  val DEFAULT_ML_OPT_MOMENTUM_RHO = 0.9

  // RMSProp params
  val ML_OPT_RMSPROP_RHO = "ml.opt.rmsprop.rho"
  val DEFAULT_ML_OPT_RMSPROP_RHO = 0.9

  // Adadelta params
  val ML_OPT_ADADELTA_RHO = "ml.opt.adadelta.rho"
  val DEFAULT_ML_OPT_ADADELTA_RHO = 0.95

  // Logistic Regression param
  val ML_LR_REG_L1 = "ml.lr.reg.l1"
  val DEFAULT_ML_LR_REG_L1 = 0.0
  val ML_LR_REG_L2 = "ml.lr.reg.l2"
  val DEFAULT_ML_LR_REG_L2 = 0.0
  val ML_REG_LOSS_TYPE = "ml.reg.loss.type"
  val DEFAULT_ML_REG_LOSS_TYPE = "loss2"
  val LR_USE_INTERCEPT = "ml.lr.use.intercept"
  val DEFAULT_LR_USE_INTERCEPT = false

  // Factorization machine M params
  val ML_FM_REG_L1_W = "ml.fm.reg.l1.w"
  val DEFAULT_ML_FM_REG_L1_W = 0.0
  val ML_FM_REG_L1_V = "ml.fm.reg.l1.v"
  val DEFAULT_ML_FM_REG_L1_V = 0.0
  val ML_FM_REG_L2_W = "ml.fm.reg.l2.w"
  val DEFAULT_ML_FM_REG_L2_W = 0.0
  val ML_FM_REG_L2_V = "ml.fm.reg.l2.v"
  val DEFAULT_ML_FM_REG_L2_V = 0.0
  val ML_FM_LEARN_TYPE = "ml.fm.learn.type"
  val DEFAULT_ML_FM_LEARN_TYPE = "r"
  val ML_FM_RANK = "ml.fm.rank"
  val DEFAULT_ML_FM_RANK = 10
  val ML_FM_V_STDDEV = "ml.fm.v.stddev"
  val DEFAULT_ML_FM_V_INIT = 0.1

  // MLR parameters
  val ML_MLR_RANK = "ml.mlr.rank"
  val DEFAULT_ML_MLR_RANK = 5
  val ML_MLR_V_INIT = "ml.mlr.v.init"
  val DEFAULT_ML_MLR_V_INIT = 0.1

  // Kmeans params
  val KMEANS_CENTER_NUM = "ml.kmeans.center.num"
  val KMEANS_SAMPLE_RATIO_PERBATCH = "ml.kmeans.sample.ratio.perbath"
  val kMEANS_C = "ml.kmeans.c"

  // MF params
  val ML_MF_RANK = "ml.mf.rank"
  val DEFAULT_ML_MF_RANK = 10
  val ML_MF_ITEM_NUM = "ml.mf.item.num"
  val DEFAULT_ML_MF_ITEM_NUM = -1
  val ML_MF_LAMBDA = "ml.mf.lambda"
  val DEFAULT_ML_MF_LAMBDA = 0.01
  val ML_MF_ETA = "ml.mf.eta"
  val DEFAULT_ML_MF_ETA = 0.1
  val ML_MF_ROW_BATCH_NUM = "ml.mf.row.batch.num"
  val DEFAULT_ML_MF_ROW_BATCH_NUM = 1

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
  val ML_GBDT_BATCH_NUM = "ml.gbdt.batch.num"
  val DEFAULT_ML_GBDT_BATCH_NUM = 10000
  val ML_GBDT_SERVER_SPLIT = "ml.gbdt.server.split"
  val DEFAULT_ML_GBDT_SERVER_SPLIT = false
  val ML_GBDT_CATE_FEAT = "ml.gbdt.cate.feat"
  val DEFAULT_ML_GBDT_CATE_FEAT = "none"

  // FTRL param
  val ML_FTRL_ALPHA = "ml.ftrl.alpha"
  val DEFAULT_ML_FTRL_ALPHA = 1
  val ML_FTRL_BETA = "ml.ftrl.beta"
  val DEFAULT_ML_FTRL_BETA = 1
  val ML_FTRL_LAMBDA1 = "ml.ftrl.lambda1"
  val DEFAULT_ML_FTRL_LAMBDA1 = 0.1
  val ML_FTRL_LAMBDA2 = "ml.ftrl.lambda2"
  val DEFAULT_ML_FTRL_LAMBDA2 = 0.1
  val ML_FTRL_BATCH_SIZE = "ml.ftrl.batch.size"
  val DEFAULT_ML_FTRL_BATCH_SIZE = 1

  /** The loss sum of all samples */
  val TRAIN_LOSS = "train.loss"
  val VALID_LOSS = "validate.loss"
  val LOG_LIKELIHOOD = "log.likelihood"

  /** The predict error of all samples */
  val TRAIN_ERROR = "train.error"
  val VALID_ERROR = "validate.error"
}

class MLConf {}
