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

package com.tencent.angel.ml.algorithm.conf

object MLConf {

  // Actions for tesla Jobs.
  // The Train action means to learn a model while Predict action uses the model to predict beyond unobserved samples.
  val ANGEL_ML_TRAIN = "train"
  val ANGEL_ML_PREDICT = "predict"
  val ANGEL_ML_INC_TRAIN = "inctrain"

  // Data params
  val ML_DATAFORMAT = "ml.data.type"
  val DEFAULT_ML_DATAFORMAT = "dummy"
  val ML_MAXDIM_PATH = "ml.maxdim.path"
  val ML_VALIDATE_RATIO = "ml.validate.ratio"
  val DEFAULT_ML_VALIDATE_RATIO = 0.05
  val ML_FEATURE_NUM = "ml.feature.num"
  val DEFAULT_ML_FEATURE_NUM = 10000
  val ANGEL_FEATURE_CONFIG = "ml.feature.conf"
  val ML_FEATURE_NNZ = "ml.feature.nnz"
  val DEFAULT_ML_FEATURE_NNZ = 10000

  // Model params
  val ML_PART_PER_SERVER = "ml.part.per.server"
  val DEFAULT_ML_PART_PER_SERVER = 5

  // Regularization params
  val ML_REG_L1 = "ml.reg.l1"
  val DEFAULT_ML_REG_L1 = 1.0
  val ML_REG_L2 = "ml.reg.l2"
  val DEFAULT_ML_REG_L2 = 1.0

  // mini-batch sgd params
  val ML_EPOCH_NUM = "ml.epochnum"
  val DEFAULT_ML_EPOCH_NUM = 50
  val ML_BATCH_NUM = "ml.batch.num"
  val DEFAULT_ML_BATCH_NUM = 5
  val ML_BATCH_SAMPLE_NUM = "ml.batch.num"
  val DEFAULT_ML_BATCH_SAMPLE_NUM = 5
  val ML_BATCH_FRACTION = "ml.batch.fraction"
  val DEFAULT_ML_BATCH_FRACTION = 0.1
  val ML_LEAR_RATE = "ml.learn.rate"
  val DEFAULT_ML_LEAR_RATE = 1.0
  val ML_LEARN_DECAY = "ml.learn.decay"
  val DEFAULT_ML_LEARN_DECAY = 0.5

  // Kmeans params
  val KMEANS_CENTER_NUM = "ml.kmeans.center.num"

  // Lasso params
  val ML_LASSO_PARAM_NAME = "ml.lasso.param.name"
  val DEFAULT_ML_LASSO_PARAM_NAME = "ml.lasso.weight"

  //Feature Hashing params

  // MF params
  val ML_MF_RANK = "ml.mf.rank"
  val ML_MF_ITEM_NUM  = "ml.mf.item.num"
  val ML_MF_NAME = "ml.mf.name"
  val ML_MF_LAMBDA = "ml.mf.lambda"
  val ML_MF_ETA = "ml.mf.eta"
  val ML_MF_ROW_BATCH_NUM = "ml.mf.row.batch.num"

  // ALS params
  val ML_ALS_USER_ROW = "ml.als.user.row"
  val ML_ALS_ITEM_ROW = "ml.als.item.row"
  val ML_ALS_USER_NAME = "ml.als.user.name"
  val ML_ALS_ITEM_NAME = "ml.als.item.name"


  // Tesla Params
  val ANGEL_ACTION_TYPE = "actionType"
  val ANGEL_MODEL_PATH  = "modelPath"
  val ANGEL_PREDICT_PATH = "angel.predict.path"

  // GBDT Params
  val ML_GBDT_TREE_NUM = "ml.gbdt.tree.num"
  val DEFAULT_ML_GBDT_TREE_NUM = 10
  val ML_GBDT_TREE_DEPTH = "ml.gbdt.tree.depth"
  val DEFAULT_ML_GBDT_TREE_DEPTH = 5
  val ML_GBDT_SPLIT_NUM = "ml.gbdt.split.num"
  val DEFAULT_ML_GBDT_SPLIT_NUM = 5
  val ML_GBDT_SAMPLE_RATIO = "ml.gbdt.sample.ratio"
  val DEFAULT_ML_GBDT_SAMPLE_RATIO= 1
  val ML_GBDT_SERVER_SPLIT = "ml.gbdt.server.split"
  val DEFAULT_ML_GBDT_SERVER_SPLIT = false

}
