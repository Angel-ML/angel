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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.core.utils.NetUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.json4s.JValue

import scala.collection.mutable

class SharedConf private() extends Serializable {

  private val dataMap: mutable.HashMap[String, String] = mutable.HashMap[String, String]()
  private var graphConf: JValue = _

  def apply(key: String): String = {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def update(key: String, value: String): SharedConf = {
    dataMap(key) = value
    this
  }

  def hasKey(key: String): Boolean = {
    dataMap.contains(key)
  }

  def allKeys(): List[String] = {
    dataMap.keys.toList
  }

  def get(key: String): String = {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def get(key: String, default: String): String = {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      default
    }
  }

  def getBoolean(key: String, default: Boolean): Boolean = {
    if (dataMap.contains(key)) {
      dataMap(key).toBoolean
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getInt(key: String, default: Int): Int = {
    if (dataMap.contains(key)) {
      dataMap(key).toInt
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getLong(key: String, default: Long): Long = {
    if (dataMap.contains(key)) {
      dataMap(key).toLong
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getFloat(key: String, default: Float): Float = {
    if (dataMap.contains(key)) {
      dataMap(key).toFloat
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getDouble(key: String, default: Double): Double = {
    if (dataMap.contains(key)) {
      dataMap(key).toDouble
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getString(key: String, default: String): String = {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      dataMap(key) = default.toString
      default
    }
  }

  def getBoolean(key: String): Boolean = {
    if (dataMap.contains(key)) {
      dataMap(key).toBoolean
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def getInt(key: String): Int = {
    if (dataMap.contains(key)) {
      dataMap(key).toInt
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def getLong(key: String): Long = {
    if (dataMap.contains(key)) {
      dataMap(key).toLong
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def getFloat(key: String): Float = {
    if (dataMap.contains(key)) {
      dataMap(key).toFloat
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def getDouble(key: String): Double = {
    if (dataMap.contains(key)) {
      dataMap(key).toDouble
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def getString(key: String): String = {
    if (dataMap.contains(key)) {
      dataMap(key)
    } else {
      throw new AngelException(s"The key: $key is not exist!")
    }
  }

  def set(key: String, value: String): SharedConf = {
    dataMap(key) = value
    this
  }

  def setBoolean(key: String, value: Boolean): SharedConf = {
    dataMap(key) = value.toString
    this
  }

  def setInt(key: String, value: Int): SharedConf = {
    dataMap(key) = value.toString
    this
  }

  def setLong(key: String, value: Long): SharedConf = {
    dataMap(key) = value.toString
    this
  }

  def setFloat(key: String, value: Float): SharedConf = {
    dataMap(key) = value.toString
    this
  }

  def setDouble(key: String, value: Double): SharedConf = {
    dataMap(key) = value.toString
    this
  }

  def setString(key: String, value: String): SharedConf = {
    dataMap(key) = value
    this
  }

  def addConf(conf: Configuration): SharedConf = {
    val iter = conf.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getKey.startsWith("angel.")) {
        dataMap(entry.getKey) = entry.getValue
      } else if (entry.getKey.startsWith("ml.")) {
        dataMap(entry.getKey) = entry.getValue
      } else if (entry.getKey.startsWith("action.")) {
        dataMap(entry.getKey) = entry.getValue
      }
    }

    this
  }

  def setJson(jast: JValue): Unit = {
    graphConf = jast
  }

  def getJson: JValue = {
    graphConf
  }
}

object SharedConf {
  val LOG = LogFactory.getLog(classOf[SharedConf])

  private var sc: SharedConf = _

  def get(): SharedConf = {
    if (sc == null) {
      sc = new SharedConf
      addMLConf()
      addAngelConf()
    }

    sc
  }

  def get(conf: Configuration): SharedConf = {
    get().addConf(conf)
  }

  private def addMLConf(): Unit = {
    val constructor = classOf[MLConf].getConstructor()
    val obj = constructor.newInstance()
    val cls = classOf[MLConf]
    cls.getDeclaredMethods.foreach { method =>
      if (!method.getName.startsWith("DEFAULT_")) {
        val key: String = method.invoke(obj).toString
        try {
          val valueMethod = cls.getMethod(s"DEFAULT_${method.getName}")
          val value: String = valueMethod.invoke(obj).toString
          sc(key) = value
        } catch {
          case _: NoSuchMethodException =>
            val key: String = method.invoke(obj).toString
            LOG.info(s"$key does not have default value!")
        }
      }
    }
  }

  private def addAngelConf(): Unit = {
    val cls = classOf[AngelConf]
    val obj = cls.getConstructor().newInstance()

    cls.getDeclaredFields.foreach { field =>
      field.setAccessible(true)
      val key = field.get(obj).toString
      if (!field.getName.startsWith("DEFAULT_")) {
        try {
          val defaultField = cls.getDeclaredField(s"DEFAULT_${field.getName}")
          defaultField.setAccessible(true)
          val defaultValue = defaultField.get(obj).toString

          sc(key) = defaultValue
        } catch {
          case _: NoSuchFieldException =>
            LOG.info(s"$key does not have default value!")
        }
      }
    }
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

    sc.get(AngelConf.ANGEL_ACTION_TYPE, AngelConf.DEFAULT_ANGEL_ACTION_TYPE)
  }

  def keyType(): String = {
    get()

    NetUtils.keyType(RowType.valueOf(sc.get(MLConf.ML_MODEL_TYPE)))
  }

  def valueType(): String = {
    get()

    NetUtils.valueType(RowType.valueOf(sc.get(MLConf.ML_MODEL_TYPE)))
  }

  def denseModelType: RowType = {
    get()

    NetUtils.getDenseModelType(RowType.valueOf(sc.get(MLConf.ML_MODEL_TYPE)))
  }

  def storageType: String = {
    get()

    NetUtils.storageType(RowType.valueOf(sc.get(MLConf.ML_MODEL_TYPE)))
  }

  def numClass: Int = {
    get()

    sc.getInt(MLConf.ML_NUM_CLASS)
  }

  def modelType: RowType = {
    get()

    RowType.valueOf(sc.get(MLConf.ML_MODEL_TYPE))
  }

  def indexRange: Long = {
    get()

    val ir = sc.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)

    if (ir == -1) {
      throw new AngelException("ML_FEATURE_INDEX_RANGE must be set!")
    } else {
      ir
    }
  }

  def inputDataFormat: String = {
    get()

    sc.getString(MLConf.ML_DATA_INPUT_FORMAT, MLConf.DEFAULT_ML_DATA_INPUT_FORMAT)
  }

  def batchSize: Int = {
    get()

    sc.getInt(MLConf.ML_MINIBATCH_SIZE, MLConf.DEFAULT_ML_MINIBATCH_SIZE)
  }

  def numUpdatePerEpoch: Int = {
    get()

    sc.getInt(MLConf.ML_NUM_UPDATE_PER_EPOCH, MLConf.DEFAULT_ML_NUM_UPDATE_PER_EPOCH)
  }

  def blockSize: Int = {
    get()

    sc.getInt(MLConf.ML_BLOCK_SIZE, MLConf.DEFAULT_ML_BLOCK_SIZE)
  }

  def epochNum: Int = {
    get()

    sc.getInt(MLConf.ML_EPOCH_NUM, MLConf.DEFAULT_ML_EPOCH_NUM)
  }

  def modelSize: Long = {
    get()

    val ms = sc.getLong(MLConf.ML_MODEL_SIZE)
    if (ms == -1) {
      indexRange
    } else {
      ms
    }
  }

  def validateRatio: Double = {
    get()

    sc.getDouble(MLConf.ML_VALIDATE_RATIO, MLConf.DEFAULT_ML_VALIDATE_RATIO)
  }

  def decay: Double = {
    get()

    sc.getDouble(MLConf.ML_LEARN_DECAY, MLConf.DEFAULT_ML_LEARN_DECAY)
  }

  def learningRate: Double = {
    get()

    sc.getDouble(MLConf.ML_LEARN_RATE, MLConf.DEFAULT_ML_LEARN_RATE)
  }

  def modelClassName: String = {
    get()

    val modelClass = sc.getString(MLConf.ML_MODEL_CLASS_NAME, MLConf.DEFAULT_ML_MODEL_CLASS_NAME)
    if (modelClass == "") {
      throw new AngelException("ml.model.class.name must be set for graph based algorithms!")
    } else {
      modelClass
    }
  }

  def useShuffle: Boolean = {
    get()

    sc.getBoolean(MLConf.ML_DATA_USE_SHUFFLE, MLConf.DEFAULT_ML_DATA_USE_SHUFFLE)
  }

  def storageLevel: String = {
    get()

    sc.get(AngelConf.ANGEL_TASK_DATA_STORAGE_LEVEL,
      AngelConf.DEFAULT_ANGEL_TASK_DATA_STORAGE_LEVEL)
  }

  def posnegRatio(): Double = {
    get()

    sc.getDouble(MLConf.ML_DATA_POSNEG_RATIO,
      MLConf.DEFAULT_ML_DATA_POSNEG_RATIO)
  }
}
