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
 */
package com.tencent.angel.ml.utils

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.matrix.RowType
import org.apache.hadoop.conf.Configuration

abstract class DataParser {
  def parse(value: String): LabeledData
}

case class DummyDataParser(splitor: String, featRange: Long, negY: Boolean, hasLable: Boolean, isClassification: Boolean, rowType: RowType) extends DataParser {
  override def parse(value: String): LabeledData = {
    if (null == value) {
      return null
    }

    var splits = value.trim.split(splitor)
    if (splits.length < 1) {
      return null
    }

    val y = if (hasLable) {
      var label = splits(0).toDouble
      if (negY && isClassification && label != 1) label = -1.0
      splits = splits.tail
      label
    } else Double.NaN

    val x = rowType match {
      case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_FLOAT_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT =>
        new SparseLongKeyDummyVector(splits.map(_.toLong), featRange)
      case _ =>
        new SparseDummyVector(splits.map(_.toInt), featRange.toInt)
    }

    new LabeledData(x, y)
  }
}

case class LibSVMDataParser(splitor: String, featRange: Long, negY: Boolean, hasLable: Boolean, isClassification: Boolean, rowType: RowType) extends DataParser {
  type V = SparseDoubleSortedVector

  override def parse(text: String): LabeledData = {
    if (null == text) {
      return null
    }

    var splits = text.trim.split(splitor)

    if (splits.length < 1)
      return null

    val y = if (hasLable) {
      var label = splits(0).toDouble
      splits = splits.tail
      if (negY && isClassification && label != 1) label = -1
      label
    } else Double.NaN
    val len = splits.length

    val x = rowType match {
      case RowType.T_DOUBLE_DENSE | RowType.T_DOUBLE_SPARSE =>
        val keys: Array[Int] = new Array[Int](len)
        val vals: Array[Double] = new Array[Double](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toInt - 1
          vals(indx2) = kv(1).toDouble
        }
        new SparseDoubleSortedVector(featRange.toInt, keys, vals)
      case RowType.T_DOUBLE_SPARSE_LONGKEY | RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT =>
        val keys: Array[Long] = new Array[Long](len)
        val vals: Array[Double] = new Array[Double](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toLong - 1
          vals(indx2) = kv(1).toDouble
        }
        new SparseLongKeySortedDoubleVector(featRange, keys, vals)
      case RowType.T_FLOAT_DENSE | RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_SPARSE_COMPONENT =>
        val keys: Array[Int] = new Array[Int](len)
        val vals: Array[Float] = new Array[Float](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toInt - 1
          vals(indx2) = kv(1).toFloat
        }
        new SparseFloatSortedVector(featRange.toInt, keys, vals)
      case RowType.T_FLOAT_SPARSE_LONGKEY =>
        val keys: Array[Long] = new Array[Long](len)
        val vals: Array[Float] = new Array[Float](len)

        // y should be +1 or -1 when classification.
        splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
          val kv = value.trim.split(":")
          keys(indx2) = kv(0).toLong - 1
          vals(indx2) = kv(1).toFloat
        }
        new SparseLongKeySortedFloatVector(featRange, keys, vals)
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y)
  }
}

case class DenseDataParser(splitor: String, featRange: Int, negY: Boolean, hasLable: Boolean, isClassification: Boolean, rowType: RowType) extends DataParser {

  override def parse(value: String): LabeledData = {
    if (null == value) {
      return null
    }

    var splits = value.trim.split(splitor)
    if (splits.length < 1) {
      return null
    }

    val y = if (hasLable) {
      var label = splits(0).toDouble
      if (negY && isClassification && label != 1) label = -1.0
      splits = splits.tail
      label
    } else 0.0

    val x = rowType match {
      case RowType.T_DOUBLE_DENSE =>
        new DenseDoubleVector(featRange, splits.map(_.toDouble))
      case RowType.T_FLOAT_DENSE =>
        new DenseFloatVector(featRange, splits.map(_.toFloat))
      case _ => throw new AngelException("RowType is not support!")
    }

    new LabeledData(x, y)
  }
}

object DataParser {
  def apply(conf: Configuration): DataParser = {
    val featRange = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
    val dataFormat = conf.get(MLConf.ML_DATA_INPUT_FORMAT, MLConf.DEFAULT_ML_DATA_FORMAT)
    val splitor = conf.get(MLConf.ML_DATA_SPLITOR, MLConf.DEFAULT_ML_DATA_SPLITOR)
    val rowType = RowType.valueOf(conf.get(MLConf.ML_MODEL_TYPE, MLConf.DEFAULT_ML_MODEL_TYPE))
    val negY = conf.getBoolean(MLConf.ML_DATA_IS_NEGY, MLConf.DEFAULT_ML_DATA_IS_NEGY)
    val hasLabel = conf.getBoolean(MLConf.ML_DATA_HAS_LABEL, MLConf.DEFAULT_ML_DATA_HAS_LABEL)
    val isClassification = conf.getBoolean(MLConf.ML_DATA_IS_CLASSIFICATION, MLConf.DEFAULT_ML_DATA_IS_CLASSIFICATION)
    dataFormat match {
      case "dummy" => DummyDataParser(splitor, featRange, negY, hasLabel, isClassification, rowType)
      case "libsvm" => LibSVMDataParser(splitor, featRange, negY, hasLabel, isClassification, rowType)
      case "dense" => DenseDataParser(splitor, featRange.toInt, negY, hasLabel, isClassification, rowType)
    }
  }
}

