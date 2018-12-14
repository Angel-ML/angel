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

import com.tencent.angel.ml.core.local.{LocalVariableProvider, OptJsonProvider}
import com.tencent.angel.ml.math2.utils.RowType
// import com.tencent.angel.model.output.format.{ColIdValueTextRowFormat, RowIdColIdValueTextRowFormat, TextColumnFormat}

object MLConf {

  // The Train action means to learn a model while Predict action uses the model to
  // predict beyond unobserved samples.
  val ANGEL_ML_TRAIN = "train"
  val ANGEL_ML_PREDICT = "predict"
  val ANGEL_ML_INC_TRAIN = "inctrain"
  val ML_ACTION_TYPE = "ml.action.type"
  val DEFAULT_ML_ACTION_TYPE: String = ANGEL_ML_TRAIN


  // Data params
  val ML_TRAIN_DATA_PATH = "angel.train.data.path"
  val DEFAULT_ML_TRAIN_DATA_PATH = ""
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
  val ML_FIELD_NUM = "ml.field.num"
  val DEFAULT_ML_FIELD_NUM = -1
  val ML_DATA_STORAGE_LEVEL = "ml.data.storage.level"
  val DEFAULT_ML_DATA_STORAGE_LEVEL = "memory"


  // Worker params
  val ANGEL_WORKER_THREAD_NUM = "angel.worker.thread.num"
  val DEFAULT_ANGEL_WORKER_THREAD_NUM = 1

  // network param
  val ANGEL_COMPRESS_BYTES = "angel.compress.bytes"
  val DEFAULT_ANGEL_COMPRESS_BYTES = 8

  // Model params
  val ML_LOAD_MODEL_PATH = "angel.load.model.path"
  val DEFAULT_ML_LOAD_MODEL_PATH = ""
  val ML_SAVE_MODEL_PATH = "angel.save.model.path"
  val DEFAULT_ML_SAVE_MODEL_PATH = ""
  val ML_MODEL_CLASS_NAME = "ml.model.class.name"
  val DEFAULT_ML_MODEL_CLASS_NAME = ""
  val ML_MODEL_SIZE = "ml.model.size"
  val DEFAULT_ML_MODEL_SIZE = -1
  val ML_MODEL_TYPE = "ml.model.type"
  val DEFAULT_ML_MODEL_TYPE = RowType.T_FLOAT_DENSE.toString
  val ML_MODEL_IS_CLASSIFICATION = "ml.model.is.classification"
  val DEFAULT_ML_MODEL_IS_CLASSIFICATION = true
  val ML_MODEL_VARIABLE_PROVIDER = "ml.model.variable.provider"
  val DEFAULT_ML_MODEL_VARIABLE_PROVIDER = classOf[LocalVariableProvider].getCanonicalName

  val ML_EPOCH_NUM = "ml.epoch.num"
  val DEFAULT_ML_EPOCH_NUM = 10
  val ML_BATCH_SAMPLE_RATIO = "ml.batch.sample.ratio"
  val DEFAULT_ML_BATCH_SAMPLE_RATIO = 1.0
  val ML_LEARN_RATE = "ml.learn.rate"
  val DEFAULT_ML_LEARN_RATE = 0.5
  val ML_LEARN_DECAY = "ml.learn.decay"
  val DEFAULT_ML_LEARN_DECAY = 0.5
  val ML_NUM_UPDATE_PER_EPOCH = "ml.num.update.per.epoch"
  val DEFAULT_ML_NUM_UPDATE_PER_EPOCH = 10
  val ML_DECAY_INTERVALS = "ml.decay.intervals"
  val DEFAULT_ML_DECAY_INTERVALS = 50
  val ML_NUM_CLASS = "ml.num.class"
  val DEFAULT_ML_NUM_CLASS = 2

  val ML_MINIBATCH_SIZE = "ml.minibatch.size"
  val DEFAULT_ML_MINIBATCH_SIZE = 128

  // Optimizer Params
  val ML_OPTIMIZER_JSON_PROVIDER = "ml.optimizer.json.provider"
  val DEFAULT_ML_OPTIMIZER_JSON_PROVIDER = classOf[OptJsonProvider].getCanonicalName

  val ML_JSON_CONF_FILE = "ml.json.conf.file"
  val DEFAULT_ML_JSON_CONF_FILE = ""

}

class MLConf {}
