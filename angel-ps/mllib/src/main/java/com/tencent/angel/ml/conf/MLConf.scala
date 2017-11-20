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

object MLConf {

  // The Train action means to learn a model while Predict action uses the model to predict beyond unobserved samples.
  val ANGEL_ML_TRAIN = "train"
  val ANGEL_ML_PREDICT = "predict"
  val ANGEL_ML_INC_TRAIN = "inctrain"

  // Data params
  val ML_DATA_FORMAT = "ml.data.type"
  val DEFAULT_ML_DATAFORMAT = "dummy"
  val ML_VALIDATE_RATIO = "ml.validate.ratio"
  val DEFAULT_ML_VALIDATE_RATIO = 0.05
  val ML_FEATURE_NUM = "ml.feature.num"
  val DEFAULT_ML_FEATURE_NUM = 10000
  val ANGEL_FEATURE_CONFIG = "ml.feature.conf"
  val ML_FEATURE_NNZ = "ml.feature.nnz"
  val DEFAULT_ML_FEATURE_NNZ = 10000
  val ML_MF_USER_OUTPUT_PATH="ml.mf.user.model.output"
  val ANGEL_PREDICT_PATH = "angel.predict.path"

  // Model params
  val ML_PART_PER_SERVER = "ml.part.per.server"
  val DEFAULT_ML_PART_PER_SERVER = 5

  // Worker params
  val ML_WORKER_THREAD_NUM = "ml.worker.thread.num"
  val DEFAULT_ML_WORKER_THREAD_NUM = 1

  // network param
  val ML_COMPRESS_BYTES = "ml.compress.bytes"
  val DEFAULT_ML_COMPRESS_BYTES = 8

  // Regularization params
  val ML_REG_L1 = "ml.reg.l1"
  val DEFAULT_ML_REG_L1 = 1.0
  val ML_REG_L2 = "ml.reg.l2"
  val DEFAULT_ML_REG_L2 = 1.0

  // mini-batch sgd params
  val ML_EPOCH_NUM = "ml.epoch.num"
  val DEFAULT_ML_EPOCH_NUM = 50
  val ML_SGD_BATCH_NUM = "ml.sgd.batch.num"
  val DEFAULT_ML_SGD_BATCH_NUM=10
  val ML_BATCH_SAMPLE_Ratio = "ml.batch.sample.ratio"
  val DEFAULT_ML_BATCH_SAMPLE_Ratio = 1.0
  val ML_LEARN_RATE = "ml.learn.rate"
  val DEFAULT_ML_LEAR_RATE = 1.0
  val ML_LEARN_DECAY = "ml.learn.decay"
  val DEFAULT_ML_LEARN_DECAY = 0.5

  // Logistic Regression param
  val LR_USE_INTERCEPT = "ml.lr.use.intercept"
  val DEFAULT_LR_USE_INTERCEPT = false
  val LR_MODEL_TYPE = "ml.lr.model.type"

  // Kmeans params
  val KMEANS_CENTER_NUM = "ml.kmeans.center.num"
  val KMEANS_SAMPLE_RATIO_PERBATCH = "ml.kmeans.sample.ratio.perbath"
  val kMEANS_C = "ml.kmeans.c"

  // Lasso params
  val ML_LASSO_PARAM_NAME = "ml.lasso.param.name"
  val DEFAULT_ML_LASSO_PARAM_NAME = "ml.lasso.weight"

  // MF params
  val ML_MF_RANK = "ml.mf.rank"
  val DEFAULT_ML_MF_RANK = 10
  val ML_MF_ITEM_NUM  = "ml.mf.item.num"
  val DEFAULT_ML_MF_ITEM_NUM = -1
  val ML_MF_LAMBDA = "ml.mf.lambda"
  val DEFAULT_ML_MF_LAMBDA = 0.01
  val ML_MF_ETA = "ml.mf.eta"
  val DEFAULT_ML_MF_ETA = 0.1
  val ML_MF_ROW_BATCH_NUM = "ml.mf.row.batch.num"
  val DEFAULT_ML_MF_ROW_BATCH_NUM = 1

  // Tesla Params
  val ANGEL_ACTION_TYPE = "actionType"

  // GBDT Params
  val ML_GBDT_TREE_NUM = "ml.gbdt.tree.num"
  val DEFAULT_ML_GBDT_TREE_NUM = 10
  val ML_GBDT_TREE_DEPTH = "ml.gbdt.tree.depth"
  val DEFAULT_ML_GBDT_TREE_DEPTH = 5
  val ML_GBDT_SPLIT_NUM = "ml.gbdt.split.num"
  val DEFAULT_ML_GBDT_SPLIT_NUM = 5
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

  // FM params
  val ML_FM_LEARN_TYPE = "ml.fm.learn.type"
  val DEFAULT_ML_FM_LEARN_TYPE = "r"
  val ML_FM_RANK = "ml.fm.rank"
  val DEFAULT_ML_FM_RANK = 10
  val ML_FM_REG0 = "ml.fm.reg0"
  val DEFAULT_ML_FM_REG0 = 0.0
  val ML_FM_REG1 = "ml.fm.reg1"
  val DEFAULT_ML_FM_REG1 = 0.0
  val ML_FM_REG2 = "ml.fm.reg2"
  val DEFAULT_ML_FM_REG2 = 0.0
  val ML_FM_V_STDDEV = "ml.fm.v.stddev"
  val DEFAULT_ML_FM_V_INIT = 0.1

  val ML_MLR_RANK = "ml.mlr.rank"
  val DEFAULT_ML_MLR_RANK = 5
  val ML_MLR_V_INIT = "ml.mlr.v.init"
  val DEFAULT_ML_MLR_V_INIT = 0.1

  // ModelParser params
  val ML_MODEL_IN_PATH = "ml.model.in.path"
  val ML_MODEL_OUT_PATH = "ml.model.out.path"
  val ML_MODEL_CONVERT_THREAD_COUNT = "ml.model.convert.thread.count"
  val DEFAULT_ML_MODEL_CONVERT_THREAD_COUNT = 1
  val ML_MODEL_NAME = "ml.model.name"


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

  /** The number of samples used to calculate the indexes */
  val TRAIN_SAMPLE_NUMBER = "train.sample.number"
  val VALIDATE_SAMPLE_NUMBER = "validate.sample.number"

  /** The loss sum of all samples */
  val TRAIN_LOSS = "train.loss"
  val VALID_LOSS = "validate.loss"
  val LOG_LIKELIHOOD = "log.likelihood"

  /** The predict error of all samples */
  val TRAIN_ERROR = "train.error"
  val VALID_ERROR = "validate.error"
}

class MLConf {}
