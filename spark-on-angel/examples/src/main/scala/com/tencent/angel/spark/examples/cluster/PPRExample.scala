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
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.rank.pagerank.ppr.PPR
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PPRExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "4").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "200").toInt
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "4")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val alpha = params.getOrElse("alpha", "0.15").toDouble // stop rate
    val gamma = params.getOrElse("gamma", "0.05").toDouble // chance that a path is to be killed
    var maxLength = params.getOrElse("maxLength", "13").toInt
    if (maxLength < 1) maxLength = math.ceil(math.log(gamma) / math.log(1- alpha)).toInt
    val epsilon = params.getOrElse("epsilon", "0.5").toDouble
    val delta = params.getOrElse("delta", "0.5").toDouble
    val topk = params.getOrElse("topk", "100").toInt
    val needReplicateEdge = params.getOrElse("needReplicateEdge", "false").toBoolean
    val seedsPath = params.getOrElse("seedsPath", null)
    val verbose = params.getOrElse("verbose", "true").toBoolean
    val nodeIdInSeedsPath = params.getOrElse("nodeIdInSeedsPath", "0").toInt
    val isSaveWalkPath = params.getOrElse("isSaveWalkPath", "false").toBoolean

    val sep = params.getOrElse("sep",  "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val ppr = new PPR()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setUseBalancePartition(useBalancePartition)
      .setBatchSize(batchSize)
      .setIsWeighted(isWeighted)
      .setPullBatchSize(pullBatchSize)
      .setNeedReplicaEdge(needReplicateEdge)
      .setTopK(topk)
      .setVerbose(verbose)
      .setIsSaveWalkPath(isSaveWalkPath)

    if (seedsPath != null) {
      val seeds = readSeeds(seedsPath, nodeIdInSeedsPath, sep, partitionNum)
      ppr.setSeeds(seeds)
    }
    ppr.setEpsilon(epsilon)
    ppr.setAlpha(alpha)
    ppr.setMaxLength(maxLength)
    ppr.setDelta(delta)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val mapping = ppr.transform(df)
      GraphIO.save(mapping, output)
    stop()
  }

  def readSeeds(path: String, nodeId: Int, sep: String, partitionNum: Int): RDD[Long] = {
    println(s"read seeds from $path.")
    GraphIO.loadNodeSeed(path, nodeId, sep).rdd
      .map(x => x.getLong(0))
      .distinct(partitionNum)
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    val executorJvmOptions = "-Djava.library.path=$JAVA_LIBRARY_PATH " +
      "-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:MaxPermSize=256m -XX:SurvivorRatio=4 -XX:+UseMembar -XX:+UseConcMarkSweepGC " +
      "-XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=4 " +
      "-XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled " +
      "-XX:CMSInitiatingOccupancyFraction=50 -XX:+UseCompressedOops"

    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    conf.setMaster(mode)
    conf.setAppName("ppr")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_ROUTER_TYPE, "hash") // only support range partition only for now
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
