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
package com.tencent.angel.spark.examples.graph

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object InitGraphExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train").toString
    val input = params.getOrElse("input", "data/bc/edge").toString
    val initBatchSize = params.getOrElse("initBatchSize", "1000000").toInt
    val psPartNum = params.getOrElse("psPartNum", "1000").toInt
    val dataPartNum = params.getOrElse("dataPartNum", "10000").toInt
    val batchItemNum = params.getOrElse("batchItemNum", "100000").toInt
    val processNum = params.getOrElse("processNum", "10000").toInt
    val sampleCount = params.getOrElse("sampleCount", "10").toInt
    val maxNodeId = params.getOrElse("maxNodeId", "282049660").toInt
    val numEdge = params.getOrElse("numEdge", "9736309911").toLong

    val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
    val sep = params.getOrElse("data.sep", "\t").toString

    println(s"input=${input} initBatchSize=${initBatchSize} psPartNum=${psPartNum} dataPartNum=${dataPartNum}")

    val conf = new SparkConf().setMaster("yarn-cluster")
    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)

    val startTs = System.currentTimeMillis()
    val data = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(sep)
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, (src, dst))
      }
    }.coalesce(dataPartNum).values.persist(storageLevel)
    //val numEdge = data.count()
    //val maxNodeId = data.map { case (src, dst) => math.max(src, dst) }.max().toLong + 1
    println(s"parse data use time = ${System.currentTimeMillis() - startTs} numEdge=$numEdge maxNodeId=$maxNodeId")

    val param = new Param()
    param.initBatchSize = initBatchSize
    param.maxIndex = maxNodeId.toInt
    param.psPartNum = psPartNum

    val neighborTable = new NeighborTable(param)
    neighborTable.initNeighbor(data, param)

    neighborTable.sampleNeighborsTest(batchItemNum, processNum, sampleCount)
    neighborTable.sampleNeighborsTest(data, batchItemNum, processNum, sampleCount)
  }
}
