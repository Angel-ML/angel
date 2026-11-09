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

import java.util

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.ml.graph.algo.embedding.embeddingpooing.{EmbeddingPoolingModel, EmbeddingPoolingParam}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.{DataStatistics, SparkUtils}
import it.unimi.dsi.fastutil.floats.FloatArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object EmbeddingPoolingWithUserSequence {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val embeddingInput = params.getOrElse("embeddingInput", "")
    val sequenceInput = params.getOrElse("sequenceInput", "")
    val output = params.getOrElse("output", "")
    val embeddingDim = params.getOrElse("embeddingDim", "32").toInt
    val batchSize = params.getOrElse("batchSize", "50").toInt
    val numPsPartitions = params.getOrElse("numPsPartitions", "50").toInt
    val withRemapping = params.getOrElse("remapping", "true").toBoolean
    val storageLevel = params.getOrElse("storageLevel", "MEMORY_ONLY")
    val numDataPartitions = params.getOrElse("numDataPartitions", "100").toInt

    val conf = new SparkConf().setAppName("EmbeddingPoolingWithSequence")
    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    conf.set(AngelConf.ANGEL_PS_BACKUP_MATRICES, "")
    conf.set(AngelConf.ANGEL_PS_BACKUP_INTERVAL_MS, "100000000")
    conf.set("io.file.buffer.size", "16000000")
    conf.set("spark.hadoop.io.file.buffer.size", "16000000")
    val executorJvmOptions = " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "

    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    val sc = new SparkContext(conf)
    val numCores = SparkUtils.getNumCores(conf)
    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    var numExecutorDataPartitions = (numCores * 3.0).toInt
    if (numDataPartitions > numExecutorDataPartitions) {
      numExecutorDataPartitions = numDataPartitions
    }
    println(s"numDataPartitions=$numExecutorDataPartitions")

    val embeddingsRDD = GraphIO.loadString(embeddingInput)
    val sequencesRDD = GraphIO.loadString(sequenceInput).repartition(numExecutorDataPartitions)
    var embeddings: RDD[(Int, Array[Float])] = null
    var maxID = 0
    var sequences: RDD[(String, Array[Int], Array[Float])] = null
    if (withRemapping) {
      val results = string2IntResults(embeddingsRDD, sequencesRDD)
      embeddings = results._1
      maxID = results._2
      sequences = results._3
    } else {
      val results = intResults(embeddingsRDD, sequencesRDD)
      embeddings = results._1
      maxID = results._2
      sequences = results._3
    }
    println("max node id is: " + maxID)

    val param = new EmbeddingPoolingParam()
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setNumPSPart(numPsPartitions)
      .setMaxIndex(maxID+1)
      .setOutputPath(output)
    val model = new EmbeddingPoolingModel(param)

    //collect app name and data size
    DataStatistics.setAppName(model.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(embeddingsRDD, "angel.embeddingInput")
    DataStatistics.calcRDDSize(sequencesRDD, "angel.sequenceInput")
    DataStatistics.setDataPartitionNum(numDataPartitions)
    DataStatistics.setPSPartitionNum(numPsPartitions)
    PSContext.getOrCreate(sc)

    model.initialize(embeddings, param)
    val result = model.fit(sequences, param)
    model.save(output, result)
    stop()
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def intResults(embeddingsRDD: RDD[String], sequencesRDD: RDD[String]): (RDD[(Int, Array[Float])],
    Int, RDD[(String, Array[Int], Array[Float])]) = {
    val conf = SparkContext.getOrCreate().getConf
    val embeddingFeatSep = conf.get("spark.angel.embedding.feature.sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "bar" => "\\|"
      case "sharp" => "#"
    }
    val userSequenceSep = conf.get("spark.angel.user.sequence.sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "bar" => "\\|"
      case "sharp" => "#"
    }
    val embeddings = embeddingsRDD.filter(f => f != null && f.length > 0).map{line =>
      val splits = line.stripLineEnd.split("\t")
      (splits(0).toInt, splits(1).split(embeddingFeatSep).map(e => e.toFloat))
    }
    val maxId = embeddings.keys.max()
    val sequences = sequencesRDD.map{ line =>
      val splits = line.stripLineEnd.split("\t")
      val uid = splits(0)
      val weights = new FloatArrayList()
      val words = new ArrayBuffer[String]()
      val wordsWeights = splits(1).split(userSequenceSep)
      wordsWeights.foreach { ele =>
        val parts = ele.split(":")
        words.append(parts(0))
        weights.add(parts(1).toFloat)
      }
      (uid, words.toArray.map(x => x.toInt), weights.toFloatArray())
    }
    (embeddings, maxId, sequences)
  }

  def string2IntResults(embeddingsRDD: RDD[String], sequencesRDD: RDD[String]): (RDD[(Int, Array[Float])],
    Int, RDD[(String, Array[Int], Array[Float])]) = {
    val conf = SparkContext.getOrCreate().getConf
    val embeddingFeatSep = conf.get("spark.angel.embedding.feature.sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "bar" => "\\|"
      case "sharp" => "#"
    }
    val userSequenceSep = conf.get("spark.angel.user.sequence.sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "bar" => "\\|"
      case "sharp" => "#"
    }
    val strings = embeddingsRDD.filter(f => f != null && f.length > 0).map{line =>
      val splits = line.stripLineEnd.split("\t")
      (splits(0), splits(1))
    }
    val stringsWithIndex = strings.map(x => x._1).map(t => (t, 1))
      .reduceByKey(_ + _).map(f => f._1).zipWithIndex().cache()
    val temp = stringsWithIndex.join(strings).map(x => x._2)
    val embeddings = temp.map(x => (x._1.toInt, x._2.split(embeddingFeatSep).map(e => e.toFloat)))
    val maxId = embeddings.keys.max()
    println("embedding dim is: " + embeddings.take(1).apply(0)._2.length)
    val sequences = sequencesRDD.map{ line =>
      val splits = line.stripLineEnd.split("\t")
      val uid = splits(0)
      val weights = new FloatArrayList()
      val words = new ArrayBuffer[String]()
      val wordsWeights = splits(1).split(userSequenceSep)
      wordsWeights.foreach { ele =>
        val parts = ele.split(":")
        words.append(parts(0))
        weights.add(parts(1).toFloat)
      }
      (uid, words.toArray, weights.toFloatArray())
    }

    val routingTable = sequences.mapPartitionsWithIndex((partId, iterator) =>
      buildRoutingTable(partId, iterator),
      true)
    val partIndex = routingTable.join(stringsWithIndex).map { case (str, (partId, index)) =>
      (partId, (str, index))
    }.groupByKey(sequences.getNumPartitions)
    val sequencesWithPartId = sequences.mapPartitionsWithIndex((partId, iterator) =>
      attachPartitionId(partId, iterator),
      true)
    val result = sequencesWithPartId.join(partIndex).map { case (_, (seqs, mapping)) =>
      val map = new util.HashMap[String, Long]()
      for ((string, index) <- mapping) {
        map.put(string, index)
      }
      seqs.map { ele =>
        val words =  ele._2.map(x => map.get(x).toInt)
        (ele._1, words, ele._3)
      }
    }.flatMap(x => x)
    (embeddings, maxId, result)
  }

  def buildRoutingTable(index: Int, iterator: Iterator[(String, Array[String], Array[Float])]): Iterator[(String, Int)] = {
    val set = new util.HashSet[String]()
    while (iterator.hasNext) {
      val ele = iterator.next()
      val words = ele._2
      for (word <- words) {
        set.add(word)
      }
    }

    val it = set.iterator()
    val result = new ArrayBuffer[(String, Int)]()
    while (it.hasNext) {
      result.append((it.next(), index))
    }

    result.iterator
  }

  def attachPartitionId(index: Int, iterator: Iterator[(String, Array[String], Array[Float])]): Iterator[(Int, Array[(String, Array[String], Array[Float])])] = {
    Iterator.single((index, iterator.toArray))
  }
}
