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


import com.tencent.angel.ml.core.utils.{MLException, RowTypeUtils}
import com.tencent.angel.ml.math2.utils.RowType
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.JObject

import scala.collection.mutable

class SharedConf private() extends Serializable {

  private val dataMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]()
  private var graphJson: JObject = _

  def apply(key: String): String = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def update(key: String, value: String): SharedConf = synchronized {
    dataMap(key) = value
    this
  }

  def hasKey(key: String): Boolean = synchronized {
    dataMap.contains(key)
  }

  def allKeys(): List[String] = synchronized {
    dataMap.keys.toList
  }

  def get(key: String): String = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def get(key: String, default: String): String = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      default
    }
  }

  def getBoolean(key: String, default: Boolean): Boolean = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toBoolean
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getInt(key: String, default: Int): Int = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toInt
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getLong(key: String, default: Long): Long = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toLong
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getFloat(key: String, default: Float): Float = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toFloat
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getDouble(key: String, default: Double): Double = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toDouble
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getString(key: String, default: String): String = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getBoolean(key: String): Boolean = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toBoolean
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def getInt(key: String): Int = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toInt
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def getLong(key: String): Long = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toLong
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def getFloat(key: String): Float = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toFloat
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def getDouble(key: String): Double = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key).toDouble
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def getString(key: String): String = synchronized {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      throw MLException(s"The key: $key is not exist!")
    }
  }

  def set(key: String, value: String): SharedConf = synchronized {
    dataMap(key) = value
    this
  }

  def setBoolean(key: String, value: Boolean): SharedConf = synchronized {
    dataMap(key) = value.toString
    this
  }

  def setInt(key: String, value: Int): SharedConf = synchronized {
    dataMap(key) = value.toString
    this
  }

  def setLong(key: String, value: Long): SharedConf = synchronized {
    dataMap(key) = value.toString
    this
  }

  def setFloat(key: String, value: Float): SharedConf = synchronized {
    dataMap(key) = value.toString
    this
  }

  def setDouble(key: String, value: Double): SharedConf = synchronized {
    dataMap(key) = value.toString
    this
  }

  def setString(key: String, value: String): SharedConf = synchronized {
    dataMap(key) = value
    this
  }

  def setJson(jast: JObject = null): Unit = synchronized {
    graphJson = jast
  }

  def getJson: JObject = synchronized {
    graphJson
  }
}

object SharedConf {
  val LOG = LogFactory.getLog("SharedConfObject")

  private var sc: SharedConf = _

  def get(): SharedConf = synchronized {
    if (sc == null) {
      sc = new SharedConf
    }

    sc
  }

  def addMap(map: Map[String, String]): Unit = {
    get()

    map.map {
      case (key: String, value: String) if key.startsWith("angel.") || key.startsWith("ml.") =>
        sc(key) = value
      case _ =>
    }
  }

  def actionType(): String = {
    get()

    "Train"
  }

  def keyType(): String = {
    get()

    RowTypeUtils.keyType(RowType.valueOf(
      sc.get(MLCoreConf.ML_MODEL_TYPE, MLCoreConf.DEFAULT_ML_MODEL_TYPE)
    ))
  }

  def valueType(): String = {
    get()

    RowTypeUtils.valueType(RowType.valueOf(
      sc.get(MLCoreConf.ML_MODEL_TYPE, MLCoreConf.DEFAULT_ML_MODEL_TYPE)
    ))
  }

  def storageType: String = {
    get()

    RowTypeUtils.storageType(RowType.valueOf(
      sc.get(MLCoreConf.ML_MODEL_TYPE, MLCoreConf.DEFAULT_ML_MODEL_TYPE)
    ))
  }

  def denseModelType: RowType = {
    get()

    RowTypeUtils.getDenseModelType(RowType.valueOf(
      sc.get(MLCoreConf.ML_MODEL_TYPE, MLCoreConf.DEFAULT_ML_MODEL_TYPE)
    ))
  }

  def numClass: Int = {
    get()

    sc.getInt(MLCoreConf.ML_NUM_CLASS, MLCoreConf.DEFAULT_ML_NUM_CLASS)
  }

  def modelType: RowType = {
    get()

    RowType.valueOf(sc.get(MLCoreConf.ML_MODEL_TYPE, MLCoreConf.DEFAULT_ML_MODEL_TYPE))
  }

  def indexRange: Long = {
    get()

    val ir = sc.getLong(MLCoreConf.ML_FEATURE_INDEX_RANGE, MLCoreConf.DEFAULT_ML_FEATURE_INDEX_RANGE)

    //    if (ir == -1) {
    //      throw MLException("ML_FEATURE_INDEX_RANGE must be set!")
    //    } else {
    //      ir
    //    }
    ir
  }

  def inputDataFormat: String = {
    get()

    sc.getString(MLCoreConf.ML_DATA_INPUT_FORMAT, MLCoreConf.DEFAULT_ML_DATA_INPUT_FORMAT)
  }

  def batchSize: Int = {
    get()

    sc.getInt(MLCoreConf.ML_MINIBATCH_SIZE, MLCoreConf.DEFAULT_ML_MINIBATCH_SIZE)
  }

  def numUpdatePerEpoch: Int = {
    get()

    sc.getInt(MLCoreConf.ML_NUM_UPDATE_PER_EPOCH, MLCoreConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)
  }

  def blockSize: Int = {
    get()

    sc.getInt(MLCoreConf.ML_BLOCK_SIZE, MLCoreConf.DEFAULT_ML_BLOCK_SIZE)
  }

  def epochNum: Int = {
    get()

    sc.getInt(MLCoreConf.ML_EPOCH_NUM, MLCoreConf.DEFAULT_ML_EPOCH_NUM)
  }

  def modelSize: Long = {
    get()

    val ms = sc.getLong(MLCoreConf.ML_MODEL_SIZE, MLCoreConf.DEFAULT_ML_MODEL_SIZE)
    if (ms == -1) {
      indexRange
    } else {
      ms
    }
  }

  def validateRatio: Double = {
    get()

    sc.getDouble(MLCoreConf.ML_VALIDATE_RATIO, MLCoreConf.DEFAULT_ML_VALIDATE_RATIO)
  }

  def decay: Double = {
    get()

    sc.getDouble(MLCoreConf.ML_LEARN_DECAY, MLCoreConf.DEFAULT_ML_LEARN_DECAY)
  }

  def learningRate: Double = {
    get()

    sc.getDouble(MLCoreConf.ML_LEARN_RATE, MLCoreConf.DEFAULT_ML_LEARN_RATE)
  }

  def modelClassName: String = {
    get()

    val modelClass = sc.getString(MLCoreConf.ML_MODEL_CLASS_NAME, MLCoreConf.DEFAULT_ML_MODEL_CLASS_NAME)
    if (modelClass == "") {
      throw MLException("ml.model.class.name must be set for graph based algorithms!")
    } else {
      modelClass
    }
  }

  def useShuffle: Boolean = {
    get()

    sc.getBoolean(MLCoreConf.ML_DATA_USE_SHUFFLE, MLCoreConf.DEFAULT_ML_DATA_USE_SHUFFLE)
  }

  def posnegRatio(): Double = {
    get()

    sc.getDouble(MLCoreConf.ML_DATA_POSNEG_RATIO,
      MLCoreConf.DEFAULT_ML_DATA_POSNEG_RATIO)
  }

  def variableProvider(): String = {
    get()

    sc.getString(MLCoreConf.ML_MODEL_VARIABLE_PROVIDER,
      MLCoreConf.DEFAULT_ML_MODEL_VARIABLE_PROVIDER
    )
  }

  def optJsonProvider(): String = {
    get()

    sc.getString(MLCoreConf.ML_OPTIMIZER_JSON_PROVIDER,
      MLCoreConf.DEFAULT_ML_OPTIMIZER_JSON_PROVIDER
    )
  }

  def storageLevel: String = {
    get()

    sc.get(MLCoreConf.ML_DATA_STORAGE_LEVEL,
      MLCoreConf.DEFAULT_ML_DATA_STORAGE_LEVEL)
  }

  def stepSizeScheduler: String = {
    get()

    sc.get(MLCoreConf.ML_DATA_STORAGE_LEVEL,
      MLCoreConf.DEFAULT_ML_DATA_STORAGE_LEVEL)
  }
}
