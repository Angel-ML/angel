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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.matrix.RowType

abstract class DataParser(val splitter: String) {
  def parse(value: String): LabeledData

  protected def processLabel(value: String, hasLabel: Boolean, isTraining: Boolean,
                             transLabel: TransLabel): (Double, String, Array[String]) = {
    if (null == value) {
      return (Double.NaN, "", null)
    }

    val splits = value.trim.split(splitter)

    if (splits.size < 1) {
      (Double.NaN, "", null)
    } else {
      if (hasLabel && isTraining) {
        val label = transLabel.trans(splits(0).toDouble)
        (label, "", splits.tail)
      } else if (hasLabel && !isTraining) {
        val attached = splits(0).trim
        (Double.NaN, attached, splits.tail)
      } else {
        (Double.NaN, "", splits)
      }
    }
  }
}

case class DummyDataParser(override val splitter: String, featRange: Long, hasLabel: Boolean, isTraining: Boolean, transLabel: TransLabel, rowType: RowType) extends DataParser(splitter) {
  override def parse(value: String): LabeledData = {

    val (y, attached, splits) = processLabel(value, hasLabel, isTraining, transLabel)
    if (splits == null) {
      return null
    }


    val keyType = NetUtils.keyType(rowType)
    val valueType = NetUtils.valueType(rowType)
    val len = splits.length
    val x = (keyType, valueType) match {
      case ("long", "double") =>
        val keys: Array[Long] = (0 until len).toArray.map(i => splits(i).toLong)
        VFactory.sparseLongKeyDoubleVector(featRange, keys, keys.map(_ => 1.0))
      // VFactory.sortedLongKeyDoubleVector(featRange, keys, keys.map(_ => 1.0))
      case ("int", "double") =>
        val keys: Array[Int] = (0 until len).toArray.map(i => splits(i).toInt)
        VFactory.sparseDoubleVector(featRange.toInt, keys, keys.map(_ => 1.0))
      // VFactory.sortedDoubleVector(featRange.toInt, keys, keys.map(_ => 1.0))
      case ("long", "float") =>
        val keys: Array[Long] = (0 until len).toArray.map(i => splits(i).toLong)
        VFactory.sparseLongKeyFloatVector(featRange, keys, keys.map(_ => 1.0f))
      // VFactory.sortedLongKeyFloatVector(featRange, keys, keys.map(_ => 1.0f))
      case ("int", "float") =>
        val keys: Array[Int] = (0 until len).toArray.map(i => splits(i).toInt)
        VFactory.sparseFloatVector(featRange.toInt, keys, keys.map(_ => 1.0f))
      // VFactory.sortedFloatVector(featRange.toInt, keys, keys.map(_ => 1.0f))
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y, attached)
  }
}

case class LibSVMDataParser(override val splitter: String, featRange: Long, hasLabel: Boolean, isTraining: Boolean, transLabel: TransLabel, rowType: RowType) extends DataParser(splitter) {

  override def parse(value: String): LabeledData = {
    val (y, attached, splits) = processLabel(value, hasLabel, isTraining, transLabel)
    if (splits == null) {
      return null
    }
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
        // VFactory.sortedLongKeyDoubleVector(featRange, keys, vals)

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
        // VFactory.sortedDoubleVector(featRange.toInt, keys, vals)
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
        // VFactory.sortedLongKeyFloatVector(featRange, keys, vals)
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
        // VFactory.sortedFloatVector(featRange.toInt, keys, vals)
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y, attached)
  }
}

case class DenseDataParser(override val splitter: String, featRange: Int, hasLabel: Boolean, isTraining: Boolean, transLabel: TransLabel, rowType: RowType) extends DataParser(splitter) {

  override def parse(value: String): LabeledData = {
    val (y, attached, splits) = processLabel(value, hasLabel, isTraining, transLabel)
    if (splits == null) {
      return null
    }

    val x = NetUtils.valueType(rowType) match {
      case "double" =>
        VFactory.denseDoubleVector(splits.map(_.toDouble))
      case "float" =>
        VFactory.denseFloatVector(splits.map(_.toFloat))
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y, attached)
  }
}

object DataParser {

  def apply(conf: SharedConf): DataParser = {
    val featRange = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
    val dataFormat = conf.get(MLConf.ML_DATA_INPUT_FORMAT, MLConf.DEFAULT_ML_DATA_INPUT_FORMAT)
    val splitter = conf.get(MLConf.ML_DATA_SPLITOR, MLConf.DEFAULT_ML_DATA_SPLITOR)
    val rowType = RowType.valueOf(conf.get(MLConf.ML_MODEL_TYPE, MLConf.DEFAULT_ML_MODEL_TYPE))
    val hasLabel = conf.getBoolean(MLConf.ML_DATA_HAS_LABEL, MLConf.DEFAULT_ML_DATA_HAS_LABEL)
    val isTraining = conf.get(AngelConf.ANGEL_ACTION_TYPE, AngelConf.DEFAULT_ANGEL_ACTION_TYPE) match {
      case "train" | "inctrain" => true
      case _ => false
    }
    val transLabel = TransLabel.get(
      conf.getString(MLConf.ML_DATA_LABEL_TRANS, MLConf.DEFAULT_ML_DATA_LABEL_TRANS),
      conf.getDouble(MLConf.ML_DATA_LABEL_TRANS_THRESHOLD, MLConf.DEFAULT_ML_DATA_LABEL_TRANS_THRESHOLD)
    )
    dataFormat match {
      case "dummy" => DummyDataParser(splitter, featRange, hasLabel, isTraining, transLabel, rowType)
      case "libsvm" => LibSVMDataParser(splitter, featRange, hasLabel, isTraining, transLabel, rowType)
      case "dense" => DenseDataParser(splitter, featRange.toInt, hasLabel, isTraining, transLabel, rowType)
    }
  }
}
