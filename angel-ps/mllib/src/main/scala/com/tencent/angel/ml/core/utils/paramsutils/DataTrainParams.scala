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


package com.tencent.angel.ml.core.utils.paramsutils

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JNothing, JValue}

class DataParams(val path: Option[String],
                 val format: Option[String],
                 val indexRange: Option[Long],
                 val numField: Option[Int],
                 val validateRatio: Option[Double],
                 val sampleRatio: Option[Double],
                 val useShuffle: Option[Boolean],
                 val posnegRatio: Option[Double],
                 val transLabel: Option[Boolean]
                ) {
  def updateConf(conf: SharedConf): Unit = {
    path.foreach(v => conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, v))
    format.foreach(v => conf.set(MLConf.ML_DATA_INPUT_FORMAT, v))
    indexRange.foreach(v => conf.setLong(MLConf.ML_FEATURE_INDEX_RANGE, v))
    numField.foreach(v => conf.setInt(MLConf.ML_FIELD_NUM, v))
    validateRatio.foreach(v => conf.setDouble(MLConf.ML_VALIDATE_RATIO, v))
    sampleRatio.foreach(v => conf.setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, v))
    useShuffle.foreach(v => conf.setBoolean(MLConf.ML_DATA_USE_SHUFFLE, v))
    posnegRatio.foreach(v => conf.setDouble(MLConf.ML_DATA_POSNEG_RATIO, v))
    transLabel.foreach(v => conf.setBoolean(MLConf.ML_DATA_TRANS_LABEL, v))
  }
}

object DataParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue): DataParams = {
    json match {
      case JNothing => new DataParams(None, None, None, None, None, None, None, None, None)
      case jast: JValue =>
        val path = jast \ ParamKeys.path match {
          case JNothing => None
          case v: JValue => Some(v.extract[String].trim)
        }

        val format = jast \ ParamKeys.format match {
          case JNothing => None
          case v: JValue => Some(v.extract[String].trim)
        }

        val indexRange = jast \ ParamKeys.indexRange match {
          case JNothing => None
          case v: JValue => Some(v.extract[Long])
        }

        val numField = jast \ ParamKeys.numField match {
          case JNothing => None
          case v: JValue => Some(v.extract[Int])
        }

        val validateRatio = jast \ ParamKeys.validateRatio match {
          case JNothing => None
          case v: JValue => Some(v.extract[Double])
        }

        val sampleRatio = jast \ ParamKeys.sampleRatio match {
          case JNothing => None
          case v: JValue => Some(v.extract[Double])
        }

        val useShuffle = jast \ ParamKeys.useShuffle match {
          case JNothing => None
          case v: JValue => Some(v.extract[Boolean])
        }

        val posnegRatio = jast \ ParamKeys.posnegRatio match {
          case JNothing => None
          case v: JValue => Some(v.extract[Double])
        }

        val transLabel = jast \ ParamKeys.transLabel match {
          case JNothing => None
          case v: JValue => Some(v.extract[Boolean])
        }

        new DataParams(path, format, indexRange, numField, validateRatio,
          sampleRatio, useShuffle, posnegRatio, transLabel)
    }
  }
}

class TrainParams(val epoch: Option[Int],
                  val numUpdatePerEpoch: Option[Int],
                  val batchSize: Option[Int],
                  val lr: Option[Double],
                  val decay: Option[Double]) {
  def updateConf(conf: SharedConf): Unit = {
    epoch.foreach(v => conf.setInt(MLConf.ML_EPOCH_NUM, v))
    numUpdatePerEpoch.foreach(v => conf.setInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, v))
    batchSize.foreach(v => conf.setInt(MLConf.ML_MINIBATCH_SIZE, v))
    lr.foreach(v => conf.setDouble(MLConf.ML_LEARN_RATE, v))
    decay.foreach(v => conf.setDouble(MLConf.ML_LEARN_DECAY, v))
  }
}

object TrainParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue): TrainParams = {
    json match {
      case JNothing => new TrainParams(None, None, None, None, None)
      case jast: JValue =>
        val epoch = jast \ ParamKeys.epoch match {
          case JNothing => None
          case v: JValue => Some(v.extract[Int])
        }

        val numUpdatePerEpoch = jast \ ParamKeys.numUpdatePerEpoch match {
          case JNothing => None
          case v: JValue => Some(v.extract[Int])
        }

        val batchSize = jast \ ParamKeys.batchSize match {
          case JNothing => None
          case v: JValue => Some(v.extract[Int])
        }

        val lr = jast \ ParamKeys.lr match {
          case JNothing => None
          case v: JValue => Some(v.extract[Double])
        }

        val decay = jast \ ParamKeys.decay match {
          case JNothing => None
          case v: JValue => Some(v.extract[Double])
        }

        new TrainParams(epoch, numUpdatePerEpoch, batchSize, lr, decay)
    }
  }
}

class ModelParams(val loadPath: Option[String],
                  val savePath: Option[String],
                  val modelType: Option[String],
                  val modelSize: Option[Long],
                  val blockSize: Option[Int]) {
  def updateConf(conf: SharedConf): Unit = {
    loadPath.foreach(v => conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, v))
    savePath.foreach(v => conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, v))
    modelType.foreach(v => conf.set(MLConf.ML_MODEL_TYPE, v))
    modelSize.foreach(v => conf.setLong(MLConf.ML_MODEL_SIZE, v))
    blockSize.foreach(v => conf.setInt(MLConf.ML_BLOCK_SIZE, v))
  }
}

object ModelParams {
  implicit val formats = DefaultFormats

  def apply(json: JValue): ModelParams = {
    json match {
      case JNothing => new ModelParams(None, None, None, None, None)
      case jast: JValue =>
        val loadPath = jast \ ParamKeys.loadPath match {
          case JNothing => None
          case v: JValue => Some(v.extract[String].trim)
        }

        val savePath = jast \ ParamKeys.savePath match {
          case JNothing => None
          case v: JValue => Some(v.extract[String].trim)
        }

        val modelType = jast \ ParamKeys.modelType match {
          case JNothing => None
          case v: JValue => Some(v.extract[String].trim)
        }

        val modelSize = jast \ ParamKeys.modelSize match {
          case JNothing => None
          case v: JValue => Some(v.extract[Long])
        }

        val blockSize = jast \ ParamKeys.blockSize match {
          case JNothing => None
          case v: JValue => Some(v.extract[Int])
        }

        new ModelParams(loadPath, savePath, modelType, modelSize, blockSize)
    }
  }
}
