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
import com.tencent.angel.graph.embedding.word2vec.{Word2VecModel, Word2VecParam, Word2VecUtils}
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Word2vecExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val loadPath = params.getOrElse("loadPath", "")
    val extraEmbeddingPath = params.getOrElse("extraEmbeddingPath", "")
    val nodeTypePath = params.getOrElse("nodeTypePath", "")
    val embeddingDim = params.getOrElse("embedding", "32").toInt
    val windowSize = params.getOrElse("window", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val numEpoch = params.getOrElse("epoch", "5").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val decayRate = params.getOrElse("decayRate", "0.5").toFloat
    val batchSize = params.getOrElse("batchSize", "50").toInt
    val logStep = params.getOrElse("logStep", "1024").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "10").toInt
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val saveMeta = params.getOrElse("saveMeta", "true").toBoolean
    val withRemapping = params.getOrElse("remapping", "false").toBoolean
    val storageLevel = params.getOrElse("storageLevel", "MEMORY_ONLY")
    val saveModelInterval = params.getOrElse("saveModelInterval", "2").toInt
    val checkpointInterval = params.getOrElse("checkpointInterval", "5").toInt
    val minCount = params.getOrElse("minCount", "0").toInt
    val dataCheckpoint = params.getOrElse("dataCheckpoint", "false").toBoolean

    val sc = start()
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
    println("RDD checkpoint dir is: " + cpDir)
    val numCores = SparkUtils.getNumCores(sc.getConf)
    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    var numDataPartitions = (numCores * 3.0).toInt
    if (dataPartitionNum > numDataPartitions) {
      numDataPartitions = dataPartitionNum
    }
    println(s"dataPartitionNum=$numDataPartitions")

    val data = GraphIO.loadString(input)
    var denseToString: Option[RDD[(Int, String)]] = None
    val corpus: RDD[Array[Int]] = if (withRemapping) {
      val temp = Word2VecUtils.corpusStringToInt(data, minCount)
      denseToString = Some(temp._2)
      temp._1
    } else {
      Word2VecUtils.corpusStringToIntWithoutRemapping(data, minCount)
    }.filter(arr => arr.length > 1)

    //start ps
    PSContext.getOrCreate(sc)

    corpus.cache()
    val trainRDD = corpus.repartition(numDataPartitions)
    trainRDD.persist(StorageLevel.fromString(storageLevel))
    corpus.unpersist()

    if (dataCheckpoint) {
      trainRDD.checkpoint()
      println("train rdd is checkpoint: " + trainRDD.isCheckpointed)
    }

    val (minWordId, maxWordId, maxLength, numDocs, numTokens) = trainRDD.mapPartitions(Word2VecUtils.summarizeApplyOp)
      .reduce(Word2VecUtils.summarizeReduceOp)
    println("train rdd is checkpoint: " + trainRDD.isCheckpointed)
    println(s"numDocs=$numDocs minWordId=$minWordId maxWordId=${maxWordId + 1} numTokens=$numTokens maxLength=$maxLength")

    val param = new Word2VecParam()
      .setLearningRate(stepSize)
      .setDecayRate(decayRate)
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setLogStep(logStep)
      .setWindowSize(windowSize)
      .setNumPSPart(Some(psPartitionNum))
      .setSeed(Random.nextInt())
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setMaxIndex(maxWordId + 1)
      .setMinIndex(minWordId)
      .setNumRowDataSet(numDocs)
      .setMaxLength(maxLength)
      .setModelCPInterval(checkpointInterval)
      .setModelSaveInterval(saveModelInterval)
      .setNodeTypePath(nodeTypePath)
      .setSaveMeta(saveMeta)
      .setModelPath(output)
      .setLoadPath(loadPath)
    val model = new Word2VecModel(param)
    if (nodeTypePath.length > 0) {
      if(withRemapping) {
        val nodeTypeRDD = GraphIO.loadString(nodeTypePath).map{ line =>
          val splits = line.stripLineEnd.split("[\\s+|,]")
          (splits(0), splits(1).toInt)
        }.distinct(numDataPartitions)
        val mapNodeTypeRDD = denseToString.get.map(x => (x._2, x._1)).join(nodeTypeRDD).map(x => x._2)
        model.initNodeType(mapNodeTypeRDD, param)
      } else {
        val nodeTypeRDD = GraphIO.loadString(nodeTypePath).map{ line =>
          val splits = line.stripLineEnd.split("[\\s+|,]")
          (splits(0).toInt, splits(1).toInt)
        }.distinct(numDataPartitions)
        model.initNodeType(nodeTypeRDD, param)
      }
    }
    if (loadPath.length > 0) {
      model.load(loadPath)
    } else {
      if (extraEmbeddingPath.length > 0) {
        val extraRDD = GraphIO.loadString(extraEmbeddingPath)
        model.extraInitialize(extraRDD, param)
      } else {
        model.randomInitialize(Random.nextInt())
      }
    }
    if (withRemapping) {
      model.deleteIfExists(output + "/mapping", SparkSession.builder().getOrCreate())
      denseToString.foreach(rdd => rdd.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping"))
    }
    model.train(trainRDD, param)
    model.save(output, numEpoch, saveMeta)
    stop()
  }

  def start(): SparkContext = {
    val conf = new SparkConf()

    // Set specific parameters for Word2Vec
    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    // Close the automatic checkpoint
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_BACKUP_AUTO_ENABLE, "false")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC, "true")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE, "false")
    conf.set(AngelConf.ANGEL_PS_BACKUP_MATRICES, "")
    conf.set("io.file.buffer.size", "16000000")
    conf.set("spark.hadoop.io.file.buffer.size", "16000000")

    // Add jvm parameters for executors
    val defaultExecutorJvmOptions = "-Djava.library.path=$JAVA_LIBRARY_PATH:/data/gaiaadmin/gaiaenv/tdwgaia/lib/native" +
      " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "
    val executorJvmOptions = conf.get("spark.executor.extraJavaOptions", defaultExecutorJvmOptions)
    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    conf.setAppName("Word2Vec")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}