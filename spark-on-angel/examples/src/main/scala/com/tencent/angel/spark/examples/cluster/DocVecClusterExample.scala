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

package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.embedding.word2vec._
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.{DataStatistics, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DocVecClusterExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val inputDict = params.getOrElse("inputDict", "")
    val inputDoc = params.getOrElse("inputDoc", "")
    val inputMap = params.getOrElse("inputMap", "")
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embedding", "32").toInt
    val batchSize = params.getOrElse("batchSize", "500").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "50").toInt
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val poolingStrategy = params.getOrElse("poolingStrategy", "mean").toUpperCase //SUM, MEAN, MAX, MIN
    val storageLevel = params.getOrElse("storageLevel", "MEMORY_ONLY")
    val colIndex = params.getOrElse("colIndex", "0").toInt

    val sc = start()

    val numCores = SparkUtils.getNumCores(sc.getConf)
    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    var numDataPartitions = (numCores * 3.0).toInt
    if (dataPartitionNum > numDataPartitions) {
      numDataPartitions = dataPartitionNum
    }
    println(s"dataPartitionNum=$numDataPartitions")

    val data = GraphIO.loadString(inputDoc, colIndex).repartition(numDataPartitions)
    val corpus: RDD[(String, Array[Int])] = if (inputMap.length > 0) {
      val mapData = GraphIO.loadString(inputMap, colIndex).repartition(numDataPartitions)
      val stringsWithIndex = mapData.filter(line => line.split(":").length > 1).map{line =>
        val splits = line.split(":")
        (splits(1), splits(0).toInt)
      }
      val temp = Word2VecUtils.corpusStringToInt(data, stringsWithIndex)
      temp
    } else {
      Word2VecUtils.corpusStringToIntWithoutRemappingWithDocId(data)
    }

    //collect app name and data size
    DataStatistics.setAppName("com.tencent.angel.spark.ml.graph.algo.embedding.word2vec.DocVecModel")
    DataStatistics.calcRDDSize(data, "angel.input.data")

    //Subsample will use ps, so start ps before subsample
    PSContext.getOrCreate(sc)

    corpus.cache()
    val trainRDD = corpus.filter(ele => ele._2.length > 1).repartition(numDataPartitions)
    trainRDD.persist(StorageLevel.fromString(storageLevel))
    corpus.unpersist()

    val inputDictRDD: RDD[String] = GraphIO.loadString(inputDict, colIndex)
    val (maxWordId, minWordId) = Word2VecUtils.getMinMaxID(inputDictRDD)
    println(s"minWordId=$minWordId maxWordId=${maxWordId + 1}")

    val param = new DocVecParam()
      .setBatchSize(batchSize)
      .setNumPSPart(Some(psPartitionNum))
      .setSeed(Random.nextInt())
      .setMaxIndex(maxWordId + 1)
      .setMinIndex(minWordId)
      .setModelPath(output)
      .setPoolingStrategy(poolingStrategy)
      .setEmbeddingDim(embeddingDim)

    val model = new DocVecModel(param)

    //init model
    model.extraInitialize(inputDictRDD, null, param)

    val result = model.action(trainRDD, param)
    GraphIO.save(result, output)
    stop()
  }

  def start(): SparkContext = {
    val conf = new SparkConf()
    /*
    conf.setMaster("local[2]")
    conf.setAppName("docvec")
    conf.set("spark.executor.instances", "2")
    */

    // Set specific parameters for DocVec
    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    // Close the automatic checkpoint
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_BACKUP_AUTO_ENABLE, "false")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC, "true")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE, "false")
    conf.set(AngelConf.ANGEL_PS_BACKUP_MATRICES, "")
    conf.set("io.file.buffer.size", "16000000")
    conf.set("spark.hadoop.io.file.buffer.size", "16000000")

    // Add jvm parameters for executors
    val defaultExecutorJvmOptions = "-Djava.library.path=$JAVA_LIBRARY_PATH" +
      " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "

    val executorJvmOptions = conf.get("spark.executor.extraJavaOptions", defaultExecutorJvmOptions)

    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    conf.setAppName("DocVec")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}