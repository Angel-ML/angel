package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.embedding.eges._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object EGESExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = "local"
    val input = params.getOrElse("input", "/Users/howiehywang/Desktop/projects/EGES-hash/EGES_test.txt")
    val output = params.getOrElse("output", "/Users/howiehywang/Desktop/projects/EGES-hash/EGES_results")
    val matrixOutput = params.getOrElse("matrixOutput", "/Users/howiehywang/Desktop/projects/EGES-hash/EGES_matOutput")
    val loadPath = params.getOrElse("loadPath", "")
    val embeddingDim = params.getOrElse("embeddingDim", "8").toInt
    val numNegSamples = params.getOrElse("numNegSamples", "1").toInt
    val numEpoch = params.getOrElse("numEpoch", "1").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val decayRate = params.getOrElse("decayRate", "0.5").toFloat
    val batchSize = params.getOrElse("batchSize", "50").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "1").toInt
    val saveModelInterval = params.getOrElse("saveModelInterval", "2").toInt
    val checkpointInterval = params.getOrElse("checkpointInterval", "5").toInt
    val weightedSI = params.getOrElse("weightedSI", "false").toBoolean // weight different SIs or not
    val numWeightsSI = params.getOrElse("numWeightsSI", "4").toInt // number of SIs
    val needRemapping = params.getOrElse("needRemapping", "true").toBoolean

    val ss = startSS(mode)

    val numDataPartitions = dataPartitionNum
    println(s"dataPartitionNum=$numDataPartitions")

    // load data: srcId + dstId + sideinfoIds, ID remapping
    var start = System.currentTimeMillis()
    val (data: RDD[Array[Int]], minNodeId, maxNodeId, minId, maxId) = if (needRemapping) {
      EGESOps.EGESDataProcess(ss, input, output, dataPartitionNum, numWeightsSI)
    } else {
      EGESOps.EGESDataProcessWithoutRemap(ss, input, dataPartitionNum, numWeightsSI)
    }

    data.persist(StorageLevel.DISK_ONLY)

    // newEdgesSI is replicated and remapped neighborTable with side information
    val newEdgesSI:RDD[(Int, Int, Array[Int])] = data.map{
      elements =>
        (elements(0), elements(1), elements.slice(2, elements.length))
    }
    newEdgesSI.persist(StorageLevel.DISK_ONLY)
    val numEdges = newEdgesSI.count()
    data.unpersist()
    var end = System.currentTimeMillis()

    println(s"the minNodeId, maxNodeId, minId, maxId and the number of edges are $minNodeId, $maxNodeId, " +
      s"$minId, $maxId, $numEdges.")
    println(s"the data processing time is ${(end - start) / 1000}s.")

    // model training
    val param = new EGESParam()
      .setLearningRate(stepSize)
      .setDecayRate(decayRate)
      .setEmbeddingDim(embeddingDim)
      .setBatchSize(batchSize)
      .setNumPSPart(Some(psPartitionNum))
      .setSeed(Random.nextInt())
      .setNumEpoch(numEpoch)
      .setNegSample(numNegSamples)
      .setNumNode(maxId - minId + 1)
      .setMinIndex(minNodeId)
      .setMaxIndex(maxNodeId)
      .setModelCPInterval(checkpointInterval)
      .setModelSaveInterval(saveModelInterval)
      .setNumWeightsSI(numWeightsSI)
      .setWeightedSI(weightedSI)
    val model = new EGESModel(param)
    if (loadPath.length > 0) {
      model.load(loadPath)
    } else {
      model.randomInitialize(Random.nextInt())
    }
    model.train(newEdgesSI, param, matrixOutput)
    model.save(matrixOutput)

    println("start computing and saving aggregateEmbedding.")
    start = System.currentTimeMillis()
    val nodeSideInfoTab = newEdgesSI.map(e => ((Array[Int](e._1) ++ e._3).mkString(" "), 1))
      .reduceByKey(_ + _).map(e => e._1).map(e => e.stripLineEnd.split(" ").map(v => v.toInt))
      .map(e => (e(0), 0, e.slice(1, e.length)))
    nodeSideInfoTab.persist(StorageLevel.DISK_ONLY)
    println(s"the size of nodeSideInfoTab is ${nodeSideInfoTab.count()}.")
    newEdgesSI.unpersist()
    val aggregateEmbedding = model.embeddingAggregate(nodeSideInfoTab, param)
    aggregateEmbedding.persist(StorageLevel.DISK_ONLY)
    println(s"the size of aggregateEmbeddings is ${aggregateEmbedding.count()}.")
    nodeSideInfoTab.unpersist()
    EGESOps.deleteIfExists(output + "/aggregateItemEmbedding", ss)
    EGESOps.save(aggregateEmbedding, output + "/aggregateItemEmbedding", " ")
    aggregateEmbedding.unpersist()
    end = System.currentTimeMillis()
    println(s"save aggregateEmbedding successfully, and the cost time is ${(end - start) / 1000}s.")

    stopSS()
  }

  def startSS(mode: String): SparkSession = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("EGES example")
    conf.set("spark.ps.model", "LOCAL")
    //conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)

    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    PSContext.getOrCreate(ss.sparkContext)
    ss
  }

  def stopSS(): Unit = {
    PSContext.stop()
    SparkSession.builder().getOrCreate().stop()
  }

}
