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
 *
 */

package com.tencent.angel.spark.ml.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}


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
      // check if indices are one-based and in ascending order
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

}
