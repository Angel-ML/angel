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


package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JNothing, JValue}
import com.tencent.angel.ml.core.utils.JsonUtils.extract

object GlobalKeys {
  val defaultOptimizer: String = "default_optimizer"
  val defaultLossFunc: String = "default_lossfunc"
  val lr: String = "lr"
  val model: String = "model"
  val train: String = "train"
  val path: String = "path"
  val format: String = "format"
  val indexRange: String = "indexrange"
  val numField: String = "numfield"
  val validateRatio: String = "validateratio"
  val sampleRatio: String = "sampleratio"
  val useShuffle: String = "useshuffle"
  val posnegRatio: String = "posnegratio"
  val transLabel: String = "translabel"
  val numClass: String = "numclass"
  val loadPath: String = "loadpath"
  val savePath: String = "savepath"
  val modelType: String = "modeltype"
  val modelSize: String = "modelsize"
  val blockSize: String = "blockSize"
  val epoch: String = "epoch"
  val numUpdatePerEpoch: String = "numupdateperepoch"
  val batchSize: String = "batchsize"
  val decay: String = "decay"
}


class DataParams(val path: Option[String],
                 val format: Option[String],
                 val indexRange: Option[Long],
                 val numField: Option[Int],
                 val validateRatio: Option[Double],
                 val sampleRatio: Option[Double],
                 val useShuffle: Option[Boolean],
                 val posnegRatio: Option[Double],
                 val transLabel: Option[String],
                 val numclass: Option[Int]
                ) {
  def updateConf(conf: SharedConf): Unit = {
    path.foreach(v => conf.set(MLCoreConf.ML_TRAIN_DATA_PATH, v))
    format.foreach(v => conf.set(MLCoreConf.ML_DATA_INPUT_FORMAT, v))
    indexRange.foreach(v => conf.setLong(MLCoreConf.ML_FEATURE_INDEX_RANGE, v))
    numField.foreach(v => conf.setInt(MLCoreConf.ML_FIELD_NUM, v))
    validateRatio.foreach(v => conf.setDouble(MLCoreConf.ML_VALIDATE_RATIO, v))
    sampleRatio.foreach(v => conf.setDouble(MLCoreConf.ML_BATCH_SAMPLE_RATIO, v))
    useShuffle.foreach(v => conf.setBoolean(MLCoreConf.ML_DATA_USE_SHUFFLE, v))
    posnegRatio.foreach(v => conf.setDouble(MLCoreConf.ML_DATA_POSNEG_RATIO, v))
    transLabel.foreach(v => conf.setString(MLCoreConf.ML_DATA_LABEL_TRANS, v))
    numclass.foreach(v => conf.setInt(MLCoreConf.ML_NUM_CLASS, v))
  }
}

object DataParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue): DataParams = {
    json match {
      case JNothing => new DataParams(None, None, None, None, None, None, None, None, None, None)
      case jast: JValue =>
        new DataParams(
          extract[String](jast, GlobalKeys.path),
          extract[String](jast, GlobalKeys.format),
          extract[Long](jast, GlobalKeys.indexRange),
          extract[Int](jast, GlobalKeys.numField),
          extract[Double](jast, GlobalKeys.validateRatio),
          extract[Double](jast, GlobalKeys.sampleRatio),
          extract[Boolean](jast, GlobalKeys.useShuffle),
          extract[Double](jast, GlobalKeys.posnegRatio),
          extract[String](jast, GlobalKeys.transLabel),
          extract[Int](jast, GlobalKeys.numClass)
        )
    }
  }
}

class TrainParams(val epoch: Option[Int],
                  val numUpdatePerEpoch: Option[Int],
                  val batchSize: Option[Int],
                  val lr: Option[Double],
                  val decay: Option[Double]) {
  def updateConf(conf: SharedConf): Unit = {
    epoch.foreach(v => conf.setInt(MLCoreConf.ML_EPOCH_NUM, v))
    numUpdatePerEpoch.foreach(v => conf.setInt(MLCoreConf.ML_NUM_UPDATE_PER_EPOCH, v))
    batchSize.foreach(v => conf.setInt(MLCoreConf.ML_MINIBATCH_SIZE, v))
    lr.foreach(v => conf.setDouble(MLCoreConf.ML_LEARN_RATE, v))
    decay.foreach(v => conf.setDouble(MLCoreConf.ML_LEARN_DECAY, v))
  }
}

object TrainParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue): TrainParams = {
    json match {
      case JNothing => new TrainParams(None, None, None, None, None)
      case jast: JValue =>
        new TrainParams(
          extract[Int](jast, GlobalKeys.epoch),
          extract[Int](jast, GlobalKeys.numUpdatePerEpoch),
          extract[Int](jast, GlobalKeys.batchSize),
          extract[Double](jast, GlobalKeys.lr),
          extract[Double](jast, GlobalKeys.decay)
        )
    }
  }
}

class ModelParams(val loadPath: Option[String],
                  val savePath: Option[String],
                  val modelType: Option[String],
                  val modelSize: Option[Long],
                  val blockSize: Option[Int]) {
  def updateConf(conf: SharedConf): Unit = {
    loadPath.foreach(v => conf.set(MLCoreConf.ML_LOAD_MODEL_PATH, v))
    savePath.foreach(v => conf.set(MLCoreConf.ML_SAVE_MODEL_PATH, v))
    modelType.foreach(v => conf.set(MLCoreConf.ML_MODEL_TYPE, v))
    modelSize.foreach(v => conf.setLong(MLCoreConf.ML_MODEL_SIZE, v))
    blockSize.foreach(v => conf.setInt(MLCoreConf.ML_BLOCK_SIZE, v))
  }
}

object ModelParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue): ModelParams = {
    json match {
      case JNothing => new ModelParams(None, None, None, None, None)
      case jast: JValue =>
        new ModelParams(
          extract[String](jast, GlobalKeys.loadPath),
          extract[String](jast, GlobalKeys.savePath),
          extract[String](jast, GlobalKeys.modelType),
          extract[Long](jast, GlobalKeys.modelSize),
          extract[Int](jast, GlobalKeys.blockSize)
        )
    }
  }
}
