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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.matrix.RowType

abstract class DataParser {
  def parse(value: String): LabeledData
}

case class DummyDataParser(splitter: String, featRange: Long, hasLabel: Boolean, transLabel: Boolean, rowType: RowType) extends DataParser {
  override def parse(value: String): LabeledData = {
    if (null == value) {
      return null
    }

    var splits = value.trim.split(splitter)
    if (splits.length < 1) {
      return null
    }

    val y = if (hasLabel) {
      val label = splits(0).toDouble
      splits = splits.tail
      if (transLabel) {
        if (label == 0) -1.0 else label
      } else label
    } else Double.NaN

    val keyType = NetUtils.keyType(rowType)
    val valueType = NetUtils.valueType(rowType)
    val len = splits.length
    val x = (keyType, valueType) match {
      case ("long", "double") =>
        val keys: Array[Long] = (0 until len).toArray.map(i => splits(i).toLong)
        VFactory.sparseLongKeyDoubleVector(featRange, keys, keys.map(_ => 1.0))
      case ("int", "double") =>
        val keys: Array[Int] = (0 until len).toArray.map(i => splits(i).toInt)
        VFactory.sparseDoubleVector(featRange.toInt, keys, keys.map(_ => 1.0))
      case ("long", "float") =>
        val keys: Array[Long] = (0 until len).toArray.map(i => splits(i).toLong)
        VFactory.sparseLongKeyFloatVector(featRange, keys, keys.map(_ => 1.0f))
      case ("int", "float") =>
        val keys: Array[Int] = (0 until len).toArray.map(i => splits(i).toInt)
        VFactory.sparseFloatVector(featRange.toInt, keys, keys.map(_ => 1.0f))
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y)
  }
}

case class LibSVMDataParser(splitter: String, featRange: Long, hasLabel: Boolean, transLabel: Boolean, rowType: RowType) extends DataParser {
  // type V = IntDoubleVector

  override def parse(text: String): LabeledData = {
    if (null == text) {
      return null
    }

    var splits = text.trim.split(splitter)

    if (splits.length < 1)
      return null

    val y = if (hasLabel) {
      val label = splits(0).toDouble
      splits = splits.tail
      if (transLabel) {
        if (label == 0) -1.0 else label
      } else label
    } else Double.NaN
    val len = splits.length

    val keyType = NetUtils.keyType(rowType)
    val valueType = NetUtils.valueType(rowType)
    val x = (keyType, valueType) match {
      case ("long", "double") =>
        val keys: Array[Long] = new Array[Long](len)
        val vals: Array[Double] = new Array[Double](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toLong - 1
          vals(indx2) = kv(1).toDouble
        }
        VFactory.sparseLongKeyDoubleVector(featRange, keys, vals)

      case ("int", "double") =>
        val keys: Array[Int] = new Array[Int](len)
        val vals: Array[Double] = new Array[Double](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toInt - 1
          vals(indx2) = kv(1).toDouble
        }
        VFactory.sparseDoubleVector(featRange.toInt, keys, vals)
      case ("long", "float") =>
        val keys: Array[Long] = new Array[Long](len)
        val vals: Array[Float] = new Array[Float](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toLong - 1
          vals(indx2) = kv(1).toFloat
        }
        VFactory.sparseLongKeyFloatVector(featRange, keys, vals)
      case ("int", "float") =>
        val keys: Array[Int] = new Array[Int](len)
        val vals: Array[Float] = new Array[Float](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toInt - 1
          vals(indx2) = kv(1).toFloat
        }
        VFactory.sparseFloatVector(featRange.toInt, keys, vals)
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y)
  }
}

case class DenseDataParser(splitter: String, featRange: Int, hasLabel: Boolean, transLabel: Boolean, rowType: RowType) extends DataParser {

  override def parse(value: String): LabeledData = {
    if (null == value) {
      return null
    }

    var splits = value.trim.split(splitter)
    if (splits.length < 1) {
      return null
    }

    val y = if (hasLabel) {
      val label = splits(0).toDouble
      splits = splits.tail
      if (transLabel) {
        if (label == 0) -1.0 else label
      } else label
    } else Double.NaN

    val x = NetUtils.valueType(rowType) match {
      case "double" =>
        VFactory.denseDoubleVector(splits.map(_.toDouble))
      case "float" =>
        VFactory.denseFloatVector(splits.map(_.toFloat))
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y)
  }
}

object DataParser {

  def apply(conf: SharedConf): DataParser = {
    val featRange = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
    val dataFormat = conf.get(MLConf.ML_DATA_INPUT_FORMAT, MLConf.DEFAULT_ML_DATA_INPUT_FORMAT)
    val splitter = conf.get(MLConf.ML_DATA_SPLITOR, MLConf.DEFAULT_ML_DATA_SPLITOR)
    val rowType = RowType.valueOf(conf.get(MLConf.ML_MODEL_TYPE, MLConf.DEFAULT_ML_MODEL_TYPE))
    val hasLabel = conf.getBoolean(MLConf.ML_DATA_HAS_LABEL, MLConf.DEFAULT_ML_DATA_HAS_LABEL)
    val transLabel = conf.getBoolean(MLConf.ML_DATA_TRANS_LABEL, MLConf.DEFAULT_ML_DATA_TRANS_LABEL)
    dataFormat match {
      case "dummy" => DummyDataParser(splitter, featRange, hasLabel, transLabel, rowType)
      case "libsvm" => LibSVMDataParser(splitter, featRange, hasLabel, transLabel, rowType)
      case "dense" => DenseDataParser(splitter, featRange.toInt, hasLabel, transLabel, rowType)
    }
  }
}
