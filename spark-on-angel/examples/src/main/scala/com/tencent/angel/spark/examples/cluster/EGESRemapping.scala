package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.examples.cluster.EGESExample.startSS
import com.tencent.angel.spark.ml.graph.algo.embedding.eges.EGESOps
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object EGESRemapping {

  def main(args: Array[String]): Unit = {
    //需要存储的文件：NodeRemapping Table, SideInfoRemapping Table, Remapped Neighbor Table
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", "") // the path of neighbor table with side information
    val output = params.getOrElse("output", "") //
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val numWeightsSI = params.getOrElse("numWeightsSI", "0").toInt // number of SIs

    val ss = startSS(mode)

    val numCores = SparkUtils.getNumCores(ss.sparkContext.getConf)
    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    var numDataPartitions = (numCores * 3.0).toInt
    if (dataPartitionNum > numDataPartitions) {
      numDataPartitions = dataPartitionNum
    }
    println(s"dataPartitionNum=$numDataPartitions")

    //do remapping
    EGESDataProcess(ss, input, output, dataPartitionNum, numWeightsSI)
    ss.stop()

  }

  def EGESDataProcess(ss: SparkSession, input: String, output: String, dataPartNum: Int,
                      numSideInfo: Int): Unit = {
    EGESOps.deleteIfExists(output + "/itemMappingTab", ss)
    EGESOps.deleteIfExists(output + "/sideInfoMappingTab", ss)
    EGESOps.deleteIfExists(output + "/remapNeighborTab", ss)
    EGESOps.deleteIfExists(output + "/remapNodeSideInfoTab", ss)

    val data: RDD[String] = ss.sparkContext.textFile(input).repartition(dataPartNum)
      .filter(line => line != null && line.length > 0)
      .map(line => (line, 1)).reduceByKey(_ + _).map(line => line._1)
    data.persist(StorageLevel.DISK_ONLY)
    // do remapping to the tables
    val tempNeigh = EGESOps.corpusStringToInt(data)
    val (remapTab, itemMappingTab, sideInfoMappingTab) = (tempNeigh._1, tempNeigh._2, tempNeigh._3)
    EGESOps.saveMap(itemMappingTab, output + "/itemMappingTab", " ")
    EGESOps.saveMap(sideInfoMappingTab, output + "/sideInfoMappingTab", " ")

    val remapNodeSideInfoTab = remapTab.map(line => Array(line(0)) ++ line.slice(2, line.length))
      .map(line => (line.mkString(" "), 1))
      .reduceByKey(_ + _)
      .map(line => line._1.split(" ").map(e => e.toInt))

    remapNodeSideInfoTab.persist(StorageLevel.DISK_ONLY)
    remapNodeSideInfoTab.count()
    EGESOps.saveRemap(remapNodeSideInfoTab, output + "/remapNodeSideInfoTab", " ")

    val remapNeiTab = remapTab.map(line => line.slice(0,2))
    remapNeiTab.persist(StorageLevel.DISK_ONLY)
    val numEdges = remapNeiTab.count()
    EGESOps.saveRemap(remapNeiTab, output + "/remapNeighborTab", " ")
    val (minNodeId, maxNodeId) = EGESOps.computeMaxMinId(remapNeiTab)
    val (minId, maxId) = EGESOps.computeMaxMinId(remapTab)

    println(s"the minNodeId, maxNodeId, minId, maxId and the number of edges are $minNodeId, " +
      s"$maxNodeId, $minId, $maxId, $numEdges.")
    remapNeiTab.unpersist()
    data.unpersist()

  }

}