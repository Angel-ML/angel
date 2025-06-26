package com.tencent.angel.graph.embedding.struct2vec

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.embedding.struct2vec.Struct2vec
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Example {
  def main(args: Array[String]): Unit = {
    val input = "data/bc/karate_club_network.txt"
    val storageLevel = StorageLevel.fromString("MEMORY_ONLY")
    val batchSize = 10
    val output = "data/output/output1"
    val stay_prob: Double = 0.3
    val opt1_reduce_len: Boolean = true
    val opt2_reduce_sim_calc: Boolean = false
    val opt3_num_layers:Int = 10
    val max_num_layers:Int = 10
    val srcIndex = 0
    val dstIndex = 1
    val weightIndex = 2
    val psPartitionNum = 1
    val partitionNum = 1
    val useEdgeBalancePartition = false
    val isWeighted = false
    val needReplicateEdge =true

    val sep = " "
    val walkLength = 10


    start()

    val struct2vec = new Struct2vec()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setWeightCol("weight")
      .setBatchSize(batchSize)
      .setWalkLength(walkLength)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setNeedReplicaEdge(needReplicateEdge)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)
      .setEpochNum(2)

    struct2vec.setOutputDir(output)
    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val mapping = struct2vec.transform(df)

    stop()
  }

  def start(mode: String = "local[4]"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("Struct2vec")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("data/cp")
    //PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}

