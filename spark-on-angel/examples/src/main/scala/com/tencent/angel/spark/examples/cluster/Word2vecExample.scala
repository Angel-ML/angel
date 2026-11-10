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
import com.tencent.angel.spark.ml.graph.algo.embedding.word2vec.Word2VecModel.{parseData, parseDataWithIndexColumn}
import com.tencent.angel.spark.ml.graph.algo.embedding.word2vec.{Word2VecModel, Word2VecModelV1, Word2VecParam, Word2VecUtils}
import com.tencent.angel.spark.ml.graph.utils.partitionstrategy.partitioners.EdgeSquarePartitionerFromPS
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.{DataLoader, DataStatistics, LogUtils, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Word2vecExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val extraInputEmbeddingPath = params.getOrElse("extraInputEmbeddingPath", "")
    val extraOutputEmbeddingPath = params.getOrElse("extraOutputEmbeddingPath", "")
    val nodeTypePath = params.getOrElse("nodeTypePath", "")
    val mapsInputPath = params.getOrElse("mapsInputPath", "")
    val embeddingDim = params.getOrElse("embedding", "32").toInt
    val windowSize = params.getOrElse("window", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val numEpoch = params.getOrElse("epoch", "5").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val decayRate = params.getOrElse("decayRate", "0.5").toFloat
    val batchSize = params.getOrElse("batchSize", "50").toInt
    val logStep = params.getOrElse("logStep", "200").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "10").toInt
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val saveOutputEmbedding = params.getOrElse("saveOutputEmbedding", "false").toBoolean
    val withSubSample = params.getOrElse("subSample", "false").toBoolean
    val withRemapping = params.getOrElse("remapping", "false").toBoolean
    val nodeTypeWithTwoLongColumns = params.getOrElse("nodeTypeWithTwoLongColumns", "false").toBoolean
    val storageLevel = params.getOrElse("storageLevel", "MEMORY_ONLY")
    val saveModelInterval = params.getOrElse("saveModelInterval", "2").toInt
    val checkpointInterval = params.getOrElse("checkpointInterval", "5").toInt
    val colIndex = params.getOrElse("colIndex", "0").toInt
    var minCount = params.getOrElse("minCount", "0").toInt
    minCount = 0
    val dataCheckpoint = params.getOrElse("dataCheckpoint", "false").toBoolean
    val gradAvg = params.getOrElse("gradAvg", "true").toBoolean
    val partitionOpt = params.getOrElse("partitionOpt", "false").toBoolean
    val localNegativeSample = params.getOrElse("localNegativeSample", "true").toBoolean
    val kernel = params.getOrElse("kernel", "2").toInt
    val mapOpt = params.getOrElse("mapOpt", "false").toBoolean
    val postMapping = if (withRemapping) params.getOrElse("postMapping", "false").toBoolean else false

    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start(partitionOpt)
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

    val data = GraphIO.loadString(input, colIndex, numDataPartitions).repartition(numDataPartitions)
    var denseToString: Option[RDD[(Int, String)]] = None

    val mapsInput = if (mapsInputPath.nonEmpty) {
      val mapsInputTmp = DataLoader.loadTable(sc, mapsInputPath, numDataPartitions, 1, null, ":")
        .rdd.filter(row => !row.anyNull).map { case Row(index: String, node: String) => (node, index.toLong) }
      mapsInputTmp
    } else null

    //Subsample will use ps, so start ps before subsample
    PSContext.getOrCreate(sc)

    val hasIndexColumn = sc.getConf.get("spark.hadoop.angel.word2vec.hasIndexColumn", "false").toBoolean
    val outputEmbeddingByNodeType = if (nodeTypePath.nonEmpty) sc.getConf.get("spark.hadoop.angel.word2vec.outputEmbeddingByNodeType", "false").toBoolean else false
    val corpus: RDD[Array[Int]] = if (withRemapping) {
      val temp = if (mapOpt) Word2VecUtils.initMappingMatrix(data, minCount, psPartitionNum, batchSize, mapsInput, hasIndexColumn)
      else Word2VecUtils.corpusStringToInt(data, minCount, mapsInput, hasIndexColumn)
      denseToString = Some(temp._2)
      temp._1
    } else {
      Word2VecUtils.corpusStringToIntWithoutRemapping(data, minCount)
    }

    //collect app name and data size
    DataStatistics.setAppName("com.tencent.angel.spark.ml.graph.algo.embedding.word2vec.Word2VecModel")
    DataStatistics.calcRDDSize(data, "angel.input.data")


    corpus.cache()
    val docs = corpus.filter(arr => arr.length > 1).repartition(numDataPartitions)
    docs.persist(StorageLevel.fromString(storageLevel))
    corpus.unpersist()

    if (dataCheckpoint) {
      docs.checkpoint()
      println("train rdd is checkpoint: " + docs.isCheckpointed)
    }
    val (minWordId, maxWordId, maxLength, numDocs, numTokens) = if (hasIndexColumn)
      docs.map(x => x.tail).mapPartitions(Word2VecUtils.summarizeApplyOp).reduce(Word2VecUtils.summarizeReduceOp)
    else docs.mapPartitions(Word2VecUtils.summarizeApplyOp).reduce(Word2VecUtils.summarizeReduceOp)
    println("train rdd is checkpoint: " + docs.isCheckpointed)
    println(s"numDocs=$numDocs minWordId=$minWordId maxWordId=${maxWordId} numTokens=$numTokens maxLength=$maxLength")

    var nodeTypeRDD: Option[RDD[(Int, Int)]] = None
    if (nodeTypePath.nonEmpty) {
      if(withRemapping) {
        val nodeTypeRDD_ = if (nodeTypeWithTwoLongColumns) {
          println(s"load nodeTypeFile with two columns")
          GraphIO.load(nodeTypePath, isWeighted = false, sep = sep)
            .select("src", "dst").rdd
            .filter(row => !row.anyNull)
            .map(row => (row.getLong(0).toString, row.getLong(1).toInt))
        } else {
          GraphIO.loadString(nodeTypePath, colIndex).map { line =>
            val splits = line.stripLineEnd.split("[\\s+|,]")
            (splits(0), splits(1).toInt)
          }
        }.distinct(numDataPartitions)
        val mapNodeTypeRDD = denseToString.get.map(x => (x._2, x._1)).join(nodeTypeRDD_).map(x => x._2)
        nodeTypeRDD = Some(mapNodeTypeRDD)
      } else {
        val nodeTypeRDD_ = if (nodeTypeWithTwoLongColumns) {
          println(s"load nodeTypeFile with two columns")
          GraphIO.load(nodeTypePath, isWeighted = false, sep = sep)
            .select("src", "dst").rdd
            .filter(row => !row.anyNull)
            .map(row => (row.getLong(0).toInt, row.getLong(1).toInt))
        } else {
          GraphIO.loadString(nodeTypePath, colIndex).map{ line =>
            val splits = line.stripLineEnd.split("[\\s+|,]")
            (splits(0).toInt, splits(1).toInt)
          }
        }.distinct(numDataPartitions)
        nodeTypeRDD = Some(nodeTypeRDD_)
      }
    }

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
      .setSaveOutputEmbedding(saveOutputEmbedding)
      .setModelPath(output)
      .setExtraInputEmbeddingPath(extraInputEmbeddingPath)
      .setExtraOutputEmbeddingPath(extraOutputEmbeddingPath)
      .setGradAvg(gradAvg)
      .setPartitionOpt(partitionOpt)
      .setLocalSample(localNegativeSample)
      .setKernel(kernel)
      .setPostMapping(postMapping)
      .setDenseToString(denseToString)
      .setPartitionNum(dataPartitionNum)
      .setHasIndexColumn(hasIndexColumn)
      .setOutputEmbeddingByNodeType(outputEmbeddingByNodeType)
      .setNodeTypeRDD(nodeTypeRDD)
    val model = if (partitionOpt) new Word2VecModelV1(param) else new Word2VecModel(param)
    if (nodeTypePath.nonEmpty) model.initNodeType(nodeTypeRDD.get, param)
    //init model
    var extraInputEmbeddingRDD: RDD[String] = null
    if (extraInputEmbeddingPath.length > 0) {
      extraInputEmbeddingRDD = GraphIO.loadString(extraInputEmbeddingPath, colIndex)
    }
    var extraOutputEmbeddingRDD: RDD[String] = null
    if (extraOutputEmbeddingPath.length > 0) {
      extraOutputEmbeddingRDD = GraphIO.loadString(extraOutputEmbeddingPath, colIndex)
    }
    if (extraInputEmbeddingRDD != null || extraOutputEmbeddingRDD != null) {
      model.extraInitialize(extraInputEmbeddingRDD, extraOutputEmbeddingRDD, param)
    } else {
      model.randomInitialize(Random.nextInt())
    }
    if (withRemapping) {
      model.deleteIfExists(output + "/mapping", SparkSession.builder().getOrCreate())
      denseToString.foreach(rdd => rdd.map(f => s"${f._1}:${f._2}").saveAsTextFile(output + "/mapping"))
    }

    if (partitionOpt) {
      val (arrayBoundsFromPS, squarePartitioner) = EdgeSquarePartitionerFromPS.intSquarePartitionerFromPS(model.psMatrix, model.modelContext, psPartitionNum, kernel)
      val trainRDD = docs.mapPartitions { iterator =>
        iterator.sliding(batchSize, batchSize).map { batch =>
          val (srcNodes, dstNodes) = if (hasIndexColumn) parseDataWithIndexColumn(batch.toArray, windowSize) else parseData(batch.toArray, windowSize)
          srcNodes.zip(dstNodes).map(x => (x, 0))
        }
      }.flatMap(x => x)
        .partitionBy(squarePartitioner)
        .map(_._1)
        .persist(StorageLevel.fromString(storageLevel))
      val numEdges = trainRDD.count()
      println(s"num training edges: $numEdges")
      docs.unpersist()
      println(s"psPartitionNum=$psPartitionNum, squarePartitioner.numPartitions = ${squarePartitioner.numPartitions}")
      model.train(trainRDD, arrayBoundsFromPS, math.sqrt(squarePartitioner.numPartitions).toInt, kernel)
    } else {
      model.train(docs, param)
    }

    val startTs = System.currentTimeMillis()
    model.save(output, numEpoch, saveOutputEmbedding)
    LogUtils.logTime(s"save use time=${System.currentTimeMillis() - startTs}")
    stop()
  }

  def start(partitionOpt: Boolean=false): SparkContext = {
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
    val defaultExecutorJvmOptions = "-Djava.library.path=$JAVA_LIBRARY_PATH" +
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