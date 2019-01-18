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


package com.tencent.angel.spark.ml.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage
import com.tencent.angel.ml.math2.vector._

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataLoader {

  // transform data to one-hot type
  def transform2OneHot(dataStr: String): (Array[Long], Double) = {

    val instances = dataStr.split(SPLIT_SEPARATOR)
    if (instances.length < 2) println(s"record length < 2, line: $dataStr")

    val label = instances.head
    val feature = instances.tail.map(_.toLong)
    (feature, label.toDouble)

  }


  def parseIntDouble(text: String, dim: Int): LabeledData = {
    if (null == text) return null

    var splits = text.trim.split(" ")

    if (splits.length < 1) return null

    var y = splits(0).toDouble
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length

    val keys = new Array[Int](len)
    val vals = new Array[Double](len)

    // y should be +1 or -1 when classification.
    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      val kv = value.trim.split(":")
      keys(indx2) = kv(0).toInt
      vals(indx2) = kv(1).toDouble
    }
    val x = VFactory.sparseDoubleVector(dim, keys, vals)
    new LabeledData(x, y)
  }

  def parseLongFloat(text: String, dim: Long): LabeledData = {
    if (null == text) return null
    var splits = text.trim.split(" ")

    if (splits.length < 1) return null

    var y = splits(0).toDouble
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length

    val keys = new Array[Long](len)
    val vals = new Array[Float](len)

    // y should be +1 or -1 when classification.
    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      val kv = value.trim.split(":")
      keys(indx2) = kv(0).toLong
      vals(indx2) = kv(1).toFloat
    }
    val x = VFactory.sparseLongKeyFloatVector(dim, keys, vals)
    new LabeledData(x, y)
  }

  def parseLongDummy(text: String, dim: Long): LabeledData = {
    if (null == text) return null
    var splits = text.trim.split(" ")
    if (splits.length < 1) return null
    var y = splits(0).toDouble
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length
    val keys = new Array[Long](len)

    splits.zipWithIndex.foreach{ case (value: String, indx2: Int) =>
      keys(indx2) = value.toLong
    }

    val x = VFactory.longDummyVector(dim, keys);
    new LabeledData(x, y)
  }

  def parseLongDouble(text: String, dim: Long): LabeledData = {
    if (null == text) return null

    var splits = text.trim.split(" ")

    if (splits.length < 1) return null

    var y = splits(0).toDouble
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length

    val keys = new Array[Long](len)
    val vals = new Array[Double](len)

    // y should be +1 or -1 when classification.
    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      val kv = value.trim.split(":")
      keys(indx2) = kv(0).toLong
      vals(indx2) = kv(1).toDouble
    }
    val x = VFactory.sparseLongKeyDoubleVector(dim, keys, vals)
    new LabeledData(x, y)
  }

  def parseIntFloat(text: String, dim: Int): LabeledData = {
    if (null == text) return null

    var splits = text.trim.split(" ")

    if (splits.length < 1) return null

    var y = splits(0).toDouble
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length

    val keys: Array[Int] = new Array[Int](len)
    val vals: Array[Float] = new Array[Float](len)

    // y should be +1 or -1 when classification.
    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      val kv = value.trim.split(":")
      keys(indx2) = kv(0).toInt
      vals(indx2) = kv(1).toFloat
    }
    val x = VFactory.sparseFloatVector(dim, keys, vals)
    new LabeledData(x, y)
  }

  def parseLabel(text: String, negLabel: Boolean): Double = {
    var label = text.trim.split(" ")(0).toDouble
    if (negLabel && label == 0) label = -1.0
    if (!negLabel && label == -1.0) label = 0.0
    return label
  }

  def parseLabel(text: String): String = {
    if (null == text) return null
    return text.trim.split(" ")(0)
  }

  def appendBias(point: LabeledData): LabeledData = {
    point.getX match {
      case x: LongDoubleVector => x.set(0L, 1.0)
      case x: IntDoubleVector => x.set(0, 1.0)
      case x: IntFloatVector => x.set(0, 1.0f)
      case x: LongFloatVector => x.set(0, 1.0f)
      case _: LongDummyVector =>
        throw new AngelException("cannot append bias for Dummy vector")
    }

    point
  }

  // transform data to sparse type
  def transform2Sparse(dataStr: String): (Array[(Long, Double)], Double) = {

    val labelFeature = dataStr.split(SPLIT_SEPARATOR)

    val featureStyled = labelFeature.tail
      .map {
        _.split(":")
      }
      .filter(x => x.length == 2)
      .map { featInfor =>
        (featInfor(0).toLong, featInfor(1).toDouble)
      }

    (featureStyled, labelFeature(0).toDouble)
  }
}
