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

package com.tencent.angel.spark.examples.local

import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.line.LINE
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

object LINEExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("LINE example")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val input = "data/bc/edge"
    val output = "model/"

    val line = new LINE()
      .setEmbedding(10)
      .setNegative(5)
      .setStepSize(0.1f)
      .setOrder(2)
      .setEpochNum(10)
      .setBatchSize(100)
      .setPartitionNum(50)
      .setPSPartitionNum(10)
      .setIsWeighted(false)
      .setRemapping(false)
      .setOutput(output)
      .setSaveMeta(false)

    val edges: DataFrame = load(input, false, " ")
    line.transform(edges)
    line.save(output, 10, false)

    PSContext.stop()
    sc.stop()
  }

  def load(input:String, isWeighted:Boolean, sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()

    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", StringType, nullable = false),
        StructField("dst", StringType, nullable = false),
        StructField("weight", FloatType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", StringType, nullable = false),
        StructField("dst", StringType, nullable = false)
      ))
    }
    ss.read
      .option("sep", sep)
      .option("header", "false")
      .schema(schema)
      .csv(input)
  }

}
