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
      sampleRate: Double,
      maxIndex: Int = -1) : DataFrame = {
    val spark = SparkSession.builder().getOrCreate()

    val instances = spark.sparkContext.textFile(input)
      .flatMap { line =>
        val items = line.split(SPLIT_SEPARATOR)
        if (items.length < 2) {
          println(s"record length < 2, line: $line")
          Iterator.empty
        } else {
          val label = items.head
          val feature = items.tail.map(_.toInt)
          if (maxIndex > 0) {
            Iterator.single(Row(label, feature.filter(x => x <= maxIndex)))
          } else {
            Iterator.single(Row(label, feature))
          }
        }
      }.repartition(partitionNum)
      .sample(false, sampleRate)
    spark.createDataFrame(instances, ONE_HOT_INSTANCE_ST)
  }

}
