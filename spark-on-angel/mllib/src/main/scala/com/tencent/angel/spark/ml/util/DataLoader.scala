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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * DataLoader loads DataFrame from HDFS/LOCAL, each line of file is split by space character.
 *
 */
object DataLoader {

  /**
   * one-hot sparse data format
   *
   * labeled data
   * 1,22,307,123
   * 0,323,333,723
   *
   * unlabeled data
   * id1,23,34,243
   * id2,33,221,233
   *
   */
  def loadOneHotInstance(
      input: String,
      partitionNum: Int,
      sampleRate: Double) : DataFrame = {
    val spark = SparkSession.builder().getOrCreate()

    val instances = spark.sparkContext.textFile(input)
      .flatMap { line =>
        val items = line.split(SPLIT_SEPARATOR)
        if (items.length < 2) {
          println(s"record length < 2, line: $line")
          Iterator.empty
        } else {
          val label = items.head
          val feature = items.tail.map(_.toLong)
          Iterator.single(Row(label, feature))
        }
      }.repartition(partitionNum)
      .sample(false, sampleRate)
    spark.createDataFrame(instances, ONE_HOT_INSTANCE_ST)
  }

  // transform data to one-hot type
  def transform2OneHot(dataRdd: RDD[String]): RDD[(Array[Long], Double)] = {

    val instances = dataRdd
      .map(line => line.trim)
      .filter(_.nonEmpty)
      .flatMap { line =>
        val items = line.split(SPLIT_SEPARATOR)
        if (items.length < 2) {
          println(s"record length < 2, line: $line")
          Iterator.empty
        } else {
          val label = items.head
          val feature = items.tail.map(_.toLong)
          Iterator.single(feature, label.toDouble)
        }
      }
    instances
  }

  // transform data to one-hot type
  def transform2OneHot(dataStr: String): (Array[Long], Double) = {

    val instances = dataStr.split(SPLIT_SEPARATOR)
    if (instances.length < 2) println(s"record length < 2, line: $dataStr")

    val label = instances.head
    val feature = instances.tail.map(_.toLong)
    (feature, label.toDouble)

  }

  def loadDense(
      input: String,
      partitionNum: Int,
      sampleRate: Double): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()


    val inputRDD = spark.sparkContext.textFile(input, partitionNum)
      .sample(false, sampleRate)
      .map(line => line.trim.split(SPLIT_SEPARATOR))

    val itemNum = inputRDD.first().length

    // create DataFrame
    val fields = ArrayBuffer[StructField]()
    for (i <- 0 until itemNum) {
      val name = "f_" + i
      val field = new StructField(name, StringType, false)
      fields += field
    }
    spark.createDataFrame(inputRDD.map(line => Row.fromSeq(line)), StructType(fields))
  }

  def loadLibsvm(
      input: String,
      partitionNum: Int,
      sampleRate: Double,
      dim: Int): RDD[(Double, Vector)] = {
    val spark = SparkSession.builder().getOrCreate()

    val withReplacement = false
    val parseRDD = spark.sparkContext.textFile(input)
      .sample(withReplacement, sampleRate)
      .repartition(partitionNum)
      .map(line => line.trim)
      .filter(_.nonEmpty)
      .map(parseLine)

    val d = if (dim > 0) {
      dim
    } else {
      parseRDD.map(record => record._2.lastOption.getOrElse(0))
        .reduce(math.max) + 1
    }
    parseRDD.map { case (label, indices, values) =>
      Tuple2(label.toDouble, Vectors.sparse(d, indices, values))
    }
  }

  private def parseLine(line: String): (String, Array[Int], Array[Double]) = {

    val indices = ArrayBuffer[Int]()
    val values = ArrayBuffer[Double]()
    val items = line.split(SPLIT_SEPARATOR).map(_.trim)
    val label = items.head

    for (item <- items.tail) {
      val ids = item.split(":")
      if (ids.length == 2) {
        indices += ids(0).toInt - 1 // convert 1-based indices to 0-based indices
        values += ids(1).toDouble
      }
      // check if indices from the input are one-based and in ascending order
      var previous = -1
      var i = 0
      val indicesLength = indices.length
      while (i < indicesLength) {
        val current = indices(i)
        require(current > previous, "indices should be one-based and in ascending order" )
        previous = current
        i += 1
      }
    }
    (label, indices.toArray, values.toArray)
  }

  // check data in type of one-hot or libsvm automatically
  def isOneHotType(input: String, sampleRate: Double, partitionNum: Int): Boolean = {

    val spark = SparkSession.builder().getOrCreate()

    val samples = spark.sparkContext.textFile(input)
      .sample(false, sampleRate)
      .repartition(partitionNum)
      .map(line => line.trim)
      .filter(_.nonEmpty)
      .take(1)

    val oneSample = samples(0).split(SPLIT_SEPARATOR).map(_.trim)
    val firstFeat = oneSample(1).split(KEY_VALUE_SEP)

    if (firstFeat.length == 1) true else false
  }

  // check the type of data for other way of loading
  def isOneHotType(dataRdd: RDD[String]): Boolean = {

    val samples = dataRdd.map(line => line.trim).filter(_.nonEmpty).take(1)

    val oneSample = samples(0).split(SPLIT_SEPARATOR).map(_.trim)
    val firstFeat = oneSample(1).split(KEY_VALUE_SEP)

    if (firstFeat.length == 1) true else false
  }

  // load SparseVector instance
  def loadSparseInstance(input: String,
                         partitionNum: Int,
                         sampleRate: Double): RDD[(Array[(Long, Double)], Double)] = {

    val spark = SparkSession.builder().getOrCreate()

    val labelAndFeature = spark.sparkContext.textFile(input)
      .sample(false, sampleRate)
      .repartition(partitionNum)
      .map(line => line.trim)
      .filter(_.nonEmpty)
      .map { dataStr =>
        val labelFeature = dataStr.split(" ")

        val feature = labelFeature.tail.map { idVal =>
          val idValArr = idVal.split(":")
          (idValArr(0).toLong, idValArr(1).toDouble)
        }

        (feature, labelFeature(0).toDouble)
      }

    labelAndFeature
  }

  // transform data to sparse type
  def transform2Sparse(dataRdd: RDD[String]): RDD[(Array[(Long, Double)], Double)] = {

    dataRdd
      .map(line => line.trim)
      .filter(_.nonEmpty)
      .map { dataStr =>
        val labelFeature = dataStr.split(SPLIT_SEPARATOR)

        val feature = labelFeature.tail.map { idVal =>
          val idValArr = idVal.split(":")

          // for test
          println("test1:" + idValArr.mkString(" "))

          (idValArr(0).toLong, idValArr(1).toDouble)
        }

        (feature, labelFeature(0).toDouble)
      }
  }

  // transform data to sparse type
  def transform2Sparse(dataStr: String): (Array[(Long, Double)], Double) = {

    val labelFeature = dataStr.split(SPLIT_SEPARATOR)

    val featureStyled = labelFeature.tail
      .map {_.split(":")}
      .filter(x => x.length == 2)
      .map{featInfor =>
        (featInfor(0).toLong, featInfor(1).toDouble)
      }

    (featureStyled, labelFeature(0).toDouble)
  }

}
