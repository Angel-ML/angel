package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.SparkUtils
import com.tencent.angel.graph.embedding.eges._
import com.tencent.angel.graph.utils.GraphIO
import org.apache.hadoop.fs.Path
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
    val matrixLoadPath = params.getOrElse("matrixLoadPath", "")
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
    val mappingPath = params.getOrElse("mappingPath", "")
    val predictInput = params.getOrElse("predictInput", "")

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
    val (newEdgesSI, minNodeId, maxNodeId, minId, maxId) =
      EGESOps.EGESDataProcess(ss, input, output, mappingPath, dataPartitionNum, numWeightsSI, needRemapping)
    var end = System.currentTimeMillis()
    println(s"the data processing time is ${(end - start) / 1000}s.")

    PSContext.getOrCreate(ss.sparkContext)

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

    var extraInputEmbeddingRDD: RDD[String] = null
    var extraContextEmbeddingRDD: RDD[String] = null
    var weightSIRDD: RDD[String] = null
    if (matrixLoadPath.nonEmpty) {
      val embeddingPath = matrixLoadPath + "/embedding/embedding"
      val contextEmbeddingPath = matrixLoadPath + "/contextEmbedding/embedding"
      val weightSIPath = matrixLoadPath + "/weightSI"
      val ss = SparkSession.builder().getOrCreate()
      if (pathIFExists(embeddingPath, ss)) {
        extraInputEmbeddingRDD = GraphIO.loadString(embeddingPath)
      }
      if (pathIFExists(contextEmbeddingPath, ss)) {
        extraContextEmbeddingRDD = GraphIO.loadString(contextEmbeddingPath)
      }
      if (pathIFExists(weightSIPath, ss)) {
        weightSIRDD = GraphIO.loadString(weightSIPath)
      }
      model.extraInitialize(extraInputEmbeddingRDD, extraContextEmbeddingRDD, weightSIRDD, param)
    } else {
      model.randomInitialize(Random.nextInt())
    }
    model.train(newEdgesSI, param, matrixOutput)
    model.save(matrixOutput)

    println("start computing and saving aggregateEmbedding.")
    start = System.currentTimeMillis()
    val predictEdgesSI = if (predictInput.nonEmpty) {
      val (predEdgesSI, minNodeIdPred, maxNodeIdPred, _, _) =
        EGESOps.EGESDataProcess(ss, predictInput, "", output, dataPartitionNum, numWeightsSI, needRemapping)
      param.setMinIndex(minNodeIdPred).setMaxIndex(maxNodeIdPred)
      predEdgesSI
    } else newEdgesSI

    val nodeSideInfoTab = predictEdgesSI.map(e => ((Array[Int](e._1) ++ e._3).mkString(" "), 1)) // remapping edges with side information
      .reduceByKey(_ + _).map(e => e._1).map(e => e.stripLineEnd.split(" ").map(v => v.toInt))
      .map(e => (e(0), minNodeId, e.slice(1, e.length)))
    nodeSideInfoTab.persist(StorageLevel.DISK_ONLY)
    println(s"the size of nodeSideInfoTab is ${nodeSideInfoTab.count()}.")
    predictEdgesSI.unpersist()
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

  private def pathIFExists(modelPath: String, ss: SparkSession): Boolean = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    fs.exists(path)
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
    val executorJvmOptions = " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "

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