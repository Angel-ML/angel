package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.SparkUtils
import com.tencent.angel.graph.embedding.eges._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object EGESExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val matrixOutput = params.getOrElse("matrixOutput", "")
    val loadPath = params.getOrElse("loadPath", "")
    val embeddingDim = params.getOrElse("embeddingDim", "32").toInt
    val numNegSamples = params.getOrElse("numNegSamples", "5").toInt
    val numEpoch = params.getOrElse("numEpoch", "5").toInt
    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val decayRate = params.getOrElse("decayRate", "0.9").toFloat
    val batchSize = params.getOrElse("batchSize", "50").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "10").toInt
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val saveModelInterval = params.getOrElse("saveModelInterval", "2").toInt
    val checkpointInterval = params.getOrElse("checkpointInterval", "5").toInt
    val weightedSI = params.getOrElse("weightedSI", "true").toBoolean // weight different SIs or not
    val numWeightsSI = params.getOrElse("numWeightsSI", "0").toInt // number of SIs
    val needRemapping = params.getOrElse("needRemapping", "false").toBoolean

    // val sc = start()
    val ss = startSS(mode)

    val numCores = SparkUtils.getNumCores(ss.sparkContext.getConf)
    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    var numDataPartitions = (numCores * 3.0).toInt
    if (dataPartitionNum > numDataPartitions) {
      numDataPartitions = dataPartitionNum
    }
    println(s"dataPartitionNum=$numDataPartitions")

    // load data: srcId + dstId + sideinfoIds, ID remapping
    var start = System.currentTimeMillis()
    val (data: RDD[Array[Int]], minNodeId, maxNodeId, minId, maxId) = if (needRemapping) {
      EGESOps.EGESDataProcess(ss, input, output, dataPartitionNum, numWeightsSI)
    } else {
      EGESOps.EGESDataProcessWithoutRemap(ss, input, dataPartitionNum, numWeightsSI)
    }
    data.persist(StorageLevel.DISK_ONLY)

    PSContext.getOrCreate(ss.sparkContext)

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
    val nodeSideInfoTab = newEdgesSI.map(e => ((Array[Int](e._1) ++ e._3).mkString(" "), 1)) // remapping edges with side information
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
    var executorJvmOptions = " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "

    // -javaagent:metricAgent.jar=useYarn=true for NGCP, which collects metrics
    if (conf.get("spark.hadoop.angel.metrics.enable", "false").toBoolean)
      executorJvmOptions += " -javaagent:metricAgent.jar=useYarn=true "

    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    conf.setAppName("EGES")
    val ss = SparkSession.builder().master(mode).config(conf).getOrCreate()
    ss
  }

  def stopSS(): Unit = {
    PSContext.stop()
    SparkSession.builder().getOrCreate().stop()
  }

}
