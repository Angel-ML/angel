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
import com.tencent.angel.ml.math2.vector._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.{SparkContext, SparkException}
import com.tencent.angel.spark.ml.util.HDFSUtils._
import scala.collection.mutable.ArrayBuffer

object DataLoader {

  def processLabel(text: String, hasLabel: Boolean, isTraining: Boolean): (Double, String, Array[String]) = {

    val splits = text.trim.split(" ")

    if (splits.size < 1) {
      (Double.NaN, "", null)
    } else {
      if (hasLabel && isTraining) {
        var label = splits(0).toDouble
        if (label == 0.0) label = -1.0
        (label, "", splits.tail)
      } else if (hasLabel && !isTraining) {
        val attached = splits(0).trim
        (attached.toDouble, attached, splits.tail)
      } else {
        (Double.NaN, "", splits)
      }
    }
  }

  def parseIntDouble(text: String, dim: Int, isTraining: Boolean = true, hasLabel: Boolean = true): LabeledData = {
    if (null == text) return null

    val (y, attached, splits) = processLabel(text, hasLabel, isTraining)
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
    new LabeledData(x, y, attached)
  }

  def parseLongFloat(text: String, dim: Long, isTraining: Boolean = true, hasLabel: Boolean = true): LabeledData = {
    if (null == text) return null

    val (y, attached, splits) = processLabel(text, hasLabel, isTraining)
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
    new LabeledData(x, y, attached)
  }

  def parseLongDummy(text: String, dim: Long, isTraining: Boolean = true, hasLabel: Boolean = true): LabeledData = {
    if (null == text) return null
    val (y, attached, splits) = processLabel(text, hasLabel, isTraining)
    val len = splits.length
    val keys = new Array[Long](len)
    val vals = new Array[Float](len)
    java.util.Arrays.fill(vals, 1.0F)

    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      keys(indx2) = value.toLong
    }

    //    val x = VFactory.longDummyVector(dim, keys)
    val x = VFactory.sparseLongKeyFloatVector(dim, keys, vals)
    new LabeledData(x, y, attached)
  }

  def parseIntDummy(text: String, dim: Int, isTraining: Boolean = true, hasLabel: Boolean = true): LabeledData = {
    if (null == text) return null
    val (y, attached, splits) = processLabel(text, hasLabel, isTraining)
    val len = splits.length
    val keys = new Array[Int](len)
    val vals = new Array[Float](len)
    java.util.Arrays.fill(vals, 1.0F)

    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      keys(indx2) = value.toInt
    }

    val x = VFactory.sparseFloatVector(dim, keys, vals)
    new LabeledData(x, y, attached)
  }

  def parseLongDouble(text: String, dim: Long, isTraining: Boolean = true, hasLabel: Boolean = true): LabeledData = {
    if (null == text) return null

    val (y, attached, splits) = processLabel(text, hasLabel, isTraining)
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
    new LabeledData(x, y, attached)
  }

  def parseIntFloat(text: String, dim: Int, isTraining: Boolean = true, hasLabel: Boolean = true): LabeledData = {
    if (null == text) return null

    val (y, attached, splits) = processLabel(text, hasLabel, isTraining)
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
    new LabeledData(x, y, attached)
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


  /**
   * load data from tdw table or hdfs;
   * sampel the data by sampleRate;
   * select the target cols by featureColExpr;
   * repartition the data in partitionNum;
   *
   * @param sc             the SparkContext
   * @param path           the input path string
   * @param partitionNum   the data partition number
   * @param sampleRate     sample rate
   * @param featureColExpr the target cols
   * @param sep            the separator for hdfs files
   * @return
   */
  def loadTable(
                 sc: SparkContext,
                 path: String,
                 partitionNum: Int,
                 sampleRate: Double,
                 featureColExpr: String,
                 sep: String): DataFrame = {
    val df = HDFSUtils.readDenseData(sc, path, partitionNum, sampleRate, sep)

    val featExpr = validateUnlabeled(featureColExpr)
    if (featExpr != defaultFeatExpr) {
      val featureSections = parseFeatureCols(featureColExpr)
      val colNames = featureSections.flatMap { case (begin, end) =>
        df.columns.slice(begin, end + 1)
      }
      df.select(colNames.head, colNames.tail: _*)
    } else {
      df
    }
  }


  def loadByLine(sc: SparkContext, path: String, partitionNum: Int, sampleRate: Double): DataFrame = {
    HDFSUtils.readByLine(sc, path, partitionNum, sampleRate)
  }


  /**
   * load data from tdw table or hdfs;the value can be null
   * sampel the data by sampleRate;
   * select the target cols by featureColExpr;
   * repartition the data in partitionNum;
   *
   * @param sc             the SparkContext
   * @param path           the input path string
   * @param partitionNum   the data partition number
   * @param sampleRate     sample rate
   * @param featureColExpr the target cols
   * @param sep            the separator for hdfs files
   * @return
   */
  def loadTableWithNull(
                         sc: SparkContext,
                         path: String,
                         partitionNum: Int,
                         sampleRate: Double,
                         featureColExpr: String,
                         sep: String): DataFrame = {
    val df = HDFSUtils.readDenseDataWithNull(sc, path, partitionNum, sampleRate, sep)

    val featExpr = validateUnlabeled(featureColExpr)
    if (featExpr != defaultFeatExpr) {
      val featureSections = parseFeatureCols(featureColExpr)
      val colNames = featureSections.flatMap { case (begin, end) =>
        df.columns.slice(begin, end + 1)
      }
      df.select(colNames.head, colNames.tail: _*)
    } else {
      df
    }
  }

  /**
   * featureInterval which represents the features column,objectCol for object column,load the features as dense vector
   *
   * @return
   */
  def loadlabeled(
                   sc: SparkContext,
                   path: String,
                   partitionNum: Int,
                   sampleRate: Double,
                   featureColExpr: String,
                   labelCol: Int,
                   isLargeTable: Boolean = false,
                   sep: String): DataFrame = {
    val (colsExpr, label) = validateLabeled(featureColExpr, labelCol)
    val featureCols = parseFeatureCols(colsExpr)
    asDataFrame(sc, path, partitionNum, sampleRate, featureCols, label, isLargeTable, sep)
  }

  private def validateLabeled(featureColExpr: String, labelCol: Int): (String, Int) = {
    val feat = if (isValidFeatExpr(featureColExpr)) featureColExpr else labeledDefaultFeatExpr
    val label = if (isValidCol(labelCol)) labelCol else 0
    (feat, label)
  }

  private def isValidCol(col: Int): Boolean = {
    col >= 0 && col != Int.MaxValue
  }


  private def validateUnlabeled(featureColExpr: String): String = {
    if (isValidFeatExpr(featureColExpr)) featureColExpr else unlabeledDefaultFeatExpr
  }

  private def isValidFeatExpr(expr: String): Boolean = {
    expr != null && expr != ""
  }


  /**
   * the string “1-10,12,15” is for 1 to 10,12 and 15 columns,
   * will return Array((1, 10), (12, 12), (15, 15))
   *
   * @param colExpr
   * @return feature sections, Array[(begin, end)]
   */
  def parseFeatureCols(colExpr: String): Array[(Int, Int)] = {
    if (colExpr == null || colExpr == "") {
      null
    } else {
      colExpr.trim.split(SEP_COMMA, -1)
        .filter(_ != "")
        .map { section =>
          val items = section.trim.split(SEP_HYPHEN, -1).map(_.toInt)
          var begin = -1
          var end = -1
          if (items.length == 1) {
            begin = items(0)
            end = items(0)
          } else if (items.length == 2) {
            begin = items(0)
            end = items(1)
            require(begin < end, s"feature col format is wrong(begin must less than end): $section")
          } else {
            new Exception(s"feature col format is wrong: $colExpr")
          }
          Tuple2(begin, end)
        }
    }
  }


  /**
   * load labeled data as label ,feature as dense vector and other non-features
   *
   * @param sc
   * @param path
   * @param partitionNum
   * @param sampleRate
   * @param featureSection
   * @param labelCol
   * @param isLargeTable which is used to decide whether we should transform data one by one
   * @return
   */

  private def asDataFrame(sc: SparkContext,
                          path: String,
                          partitionNum: Int,
                          sampleRate: Double,
                          featureSection: Array[(Int, Int)],
                          labelCol: Int,
                          isLargeTable: Boolean = false,
                          sep: String): DataFrame = {
    var outputDF: DataFrame = null

    // HDFS file or local file
    if (HDFSUtils.isSparse(sc, path, sep)) {
      val dim = sparseDim(featureSection)
      outputDF = HDFSUtils.readLibsvmData(sc, path, partitionNum, sampleRate, dim, labelCol, sep)

    } else {
      val inputDF = HDFSUtils.readDenseData(sc, path, partitionNum, sampleRate, sep)
      outputDF = if (isLargeTable)
        transformByRdd(inputDF, featureSection, labelCol)
      else
        transform(inputDF, featureSection, labelCol)
    }

    LogUtils.logTime(s"input data num: ${outputDF.count()}")

    outputDF
  }

  private def sparseDim(featureSection: Array[(Int, Int)]): Int = {
    if (featureSection.length == 0) {
      -1
    }
    else if (featureSection.length == 1) {
      if (featureSection(0)._2 == Int.MaxValue) {
        -1
      } else {
        featureSection(0)._2 - featureSection(0)._1 + 1
      }
    }
    else {
      new SparkException("Sparse train data feature expression must be \"i-j\" or empty")
      // this 0 value means nothing, program have exit before here.
      0
    }
  }


  /**
   * this function is different from the front,the data structure is composed by rdd which is
   * used for table with large column
   *
   * @param df
   * @param featureSection if don't know about the number of feature cols,we will choose all of them automatically
   * @param labelCol
   * @return
   */
  private def transformByRdd(df: DataFrame, featureSection: Array[(Int, Int)], labelCol: Int): DataFrame = {

    // change the order of StructField
    val oriFields = df.schema.fields
    val fieldNum = oriFields.length

    val arrayCol = featureSection
      .flatMap { case (begin, end) =>
        var endNew = end
        if (end == Int.MaxValue) endNew = fieldNum
        begin to endNew
      }
      .filter(x => x < fieldNum)

    val notFeatures = (0 until fieldNum).filterNot(x => arrayCol.contains(x)).toArray

    val newFields = new ArrayBuffer[StructField]
    // vector feature
    newFields += StructField(DFStruct.FEATURE, VectorType, false)
    // non features
    newFields ++= notFeatures.map(ele => oriFields(ele))

    val rdd = df.rdd.map { case row: Row =>
      val arrayX = row.toSeq
      var newArray = new ArrayBuffer[Any]

      newArray += Vectors.dense(arrayCol.map(ele => arrayX(ele).toString.toDouble))
      newArray ++= notFeatures.map(ele => arrayX(ele))
      Row.fromSeq(newArray)
    }

    val sqlContext = df.sqlContext
    var labelFeatureDF = sqlContext.createDataFrame(rdd, StructType(newFields))

    if (isValidCol(labelCol)) {
      val labelColName = oriFields(labelCol).name

      labelFeatureDF = labelFeatureDF.withColumnRenamed(labelColName, DFStruct.LABEL)
        .withColumn(DFStruct.LABEL, col(DFStruct.LABEL).cast(DoubleType))
    }

    labelFeatureDF

  }


  /**
   * the feature cols are assigned by featureInterval not the begin and end,
   * and the features in featureInterval compose of Vector
   */
  private def transform(df: DataFrame, featureSection: Array[(Int, Int)], labelCol: Int): DataFrame = {
    //change the order of StructField
    val oriFields = df.schema.fields

    val featCols = featureSection.flatMap { case (begin, end) =>
      var endNew = end
      if (end == Int.MaxValue) endNew = endNew - 1
      val result = oriFields.slice(begin, endNew + 1).map(x => col(x.name))
      result
    }

    val featColNames = featureSection.flatMap { case (begin, end) =>
      var endNew = end
      if (end == Int.MaxValue) endNew = endNew - 1

      oriFields.slice(begin, endNew + 1).map(x => x.name)
    }.toSet

    val extractFeat = udf { (items: Seq[Any]) => Vectors.dense(items.toArray.map(_.toString.toDouble)) }

    var structDF = df.withColumn(DFStruct.FEATURE, functions.array(featCols: _*))
      .withColumn(DFStruct.FEATURE, extractFeat(functions.col(DFStruct.FEATURE)))

    // remove featureSection columns in DataFrame
    val filteredCols = structDF.columns.filter(x => !featColNames.contains(x))
    structDF = structDF.select(filteredCols.head, filteredCols.tail: _*)

    if (isValidCol(labelCol)) {
      val labelColName = oriFields(labelCol).name

      structDF = structDF.withColumnRenamed(labelColName, DFStruct.LABEL)
        .withColumn(DFStruct.LABEL, col(DFStruct.LABEL).cast(DoubleType))
    }
    structDF
  }

}
