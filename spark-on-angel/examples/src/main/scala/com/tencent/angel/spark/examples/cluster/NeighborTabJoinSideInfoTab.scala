package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.examples.cluster.EGESExample.startSS
import com.tencent.angel.spark.ml.graph.algo.embedding.eges.EGESOps
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

object NeighborTabJoinSideInfoTab {

  def main(args: Array[String]): Unit = {
    //实现临接表与side information表join功能
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input1 = params.getOrElse("input1", "")
    val input2 = params.getOrElse("input2", "")
    val output = params.getOrElse("output", "")
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val batchSize = params.getOrElse("batchSize", "200").toInt
    val windowSize = params.getOrElse("windowSize", "10").toInt

    val ss = startSS(mode)
    EGESOps.deleteIfExists(output, ss)

    val walkPaths = ss.sparkContext.textFile(input1, dataPartitionNum).repartition(dataPartitionNum)
      .filter(line => line != null && line.length > 0)
      .map(line => line.stripLineEnd.split("[\\s+|,]"))
      .map(e => e.map(ele => ele.toInt))
    // transform walkPaths to neighborTab
    val neighborTab = walkPaths.mapPartitions{
      iter =>
        iter.sliding(batchSize, batchSize)
          .map(batch => parseBatchData(batch.toArray, windowSize))
          .flatMap(e => e)
    }
    neighborTab.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"the size of neighborTab is ${neighborTab.count()}.")

    val sideInfoTab = ss.sparkContext.textFile(input2, dataPartitionNum).repartition(dataPartitionNum).distinct()
      .filter(line => line != null && line.length > 0)
      .map(line => line.stripLineEnd.split("[\\s+|,]"))
      .map(e => e.map(ele => ele.toInt))
      .map(e => (e(0), e.slice(1, e.length)))
    sideInfoTab.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"the size of sideInfoTab is ${sideInfoTab.count()}.")

    //join
    val neighborTabWithSideInfo = neighborTab.join(sideInfoTab).map(e => Array(e._1) ++ Array(e._2._1) ++ e._2._2)
    println(s"the size of neighborTabWithSideInfo is ${neighborTabWithSideInfo.count()}.")

    //save neighborTabWithSideInfo
    EGESOps.saveRemap(neighborTabWithSideInfo, output, " ")
    neighborTab.unpersist()
    sideInfoTab.unpersist()

    ss.stop()
  }

  def parseBatchData(sentences: Array[Array[Int]], windowSize: Int): Array[(Int, Int)] = {
    val neighborTab = new ArrayBuffer[(Int, Int)]()
    for (s <- sentences.indices) {
      val sen = sentences(s)
      for (srcIndex <- sen.indices) {
        var dstIndex = Math.max(srcIndex - windowSize, 0)
        while (dstIndex < Math.min(srcIndex + windowSize + 1, sen.length)) {
          if (srcIndex != dstIndex) {
            neighborTab.append((sen(dstIndex), sen(srcIndex)))
          }
          dstIndex += 1
        }
      }
    }
    neighborTab.toArray
  }
}