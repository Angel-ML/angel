package com.tencent.angel.graph.embedding.struc2vec.test

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.graph.embedding.struc2vec.test.Struc2Vec
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Struc2VecExample {
    def main(args: Array[String]): Unit = {
      val input = "data/bc/karate_club_network.txt"
      val storageLevel = StorageLevel.fromString("MEMORY_ONLY")
      val batchSize = 10
      val output = "data/output/output1"
      val srcIndex = 0
      val dstIndex = 1
      val weightIndex = 2
      val psPartitionNum = 1
      val partitionNum = 1
      val useEdgeBalancePartition = false
      val isWeighted = false
      val needReplicateEdge = true

      val sep = " "
      val walkLength = 10


      start()

      val struc2Vec = new Struc2Vec()
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
        .setEpochNum(3)

      struc2Vec.setOutputDir(output)
      val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
      val mapping = struc2Vec.transform(df)

      mapping.show()

      stop()
    }

    def start(mode: String = "local[4]"): Unit = {
      val conf = new SparkConf()
      conf.setMaster(mode)
      conf.setAppName("Struc2Vec")
      conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      sc.setCheckpointDir("data/cp")
      //PSContext.getOrCreate(sc)
    }


    def stop(): Unit = {

    }

}



