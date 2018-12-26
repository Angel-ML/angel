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


package com.tencent.angel.spark.context

import java.util
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.client.AngelContext
import com.tencent.angel.common.location.Location
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.conf.AngelConf._
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta, RowType}
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.ParameterServer
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

import org.apache.commons.httpclient.URI

import com.tencent.angel.ps.storage.partitioner.Partitioner

/**
  * AngelPSContext for driver and executor, it is an implement of `PSContext`
  */
private[spark] class AngelPSContext(contextId: Int, angelCtx: AngelContext) extends PSContext {

  import AngelPSContext._

  private val matrixMetaMap = mutable.HashMap.empty[Int, MatrixMeta]
  private val psVectorPools = new ConcurrentHashMap[Int, PSVectorPool]()
  // Angel configuration that is delivered to executors by spark driver
  private val angelConf = {
    val map = mutable.HashMap.empty[String, String]

    map.put(MASTER_IP, angelCtx.getMasterLocation.getIp)
    map.put(MASTER_PORT, angelCtx.getMasterLocation.getPort.toString)

    val hadoopConf = angelCtx.getConf
    val keys = ArrayBuffer.empty[String]
    for (entry <- hadoopConf.asScala) {
      map.put(entry.getKey, entry.getValue)
      keys += entry.getKey
    }
    map.put(CONF_KEYS, keys.mkString(";"))
    map
  }
  private val psAgent = {
    val masterIp = angelCtx.getMasterLocation.getIp
    val masterPort = angelCtx.getMasterLocation.getPort
    new PSAgent(angelCtx.getConf, masterIp, masterPort, contextId, false, null)
  }
  private var matrixCounter = 0

  def createVector(dimension: Long,
                   t: RowType = RowType.T_DOUBLE_DENSE,
                   poolCapacity: Int = PSVectorPool.DEFAULT_POOL_CAPACITY,
                   range: Long,
                   additionalConfiguration:Map[String, String] = Map()): PSVector = {
    assertCallByDriver("The operation of creating a vector can only be called on the driver side.")
    createVectorPool(dimension, poolCapacity, t, range, additionalConfiguration).allocate()
  }

  private[spark] def createVectorPool(dimension: Long,
                                      capacity: Int,
                                      t: RowType,
                                      range: Long,
                                      additionalConfiguration:Map[String, String]): PSVectorPool = {
    val thisCapacity = if (capacity > 0) capacity else PSVectorPool.DEFAULT_POOL_CAPACITY
    val matrixMeta = if (t.isDense)
      PSMatrix.dense(thisCapacity, dimension, -1, -1, t, additionalConfiguration)
    else
      PSMatrix.sparse(thisCapacity, dimension, range, t, additionalConfiguration)
    val pool = new PSVectorPool(matrixMeta.id, dimension, thisCapacity, t)
    psVectorPools.put(pool.id, pool)
    pool
  }

  def createDenseMatrix(rows: Int,
                        cols: Long,
                        rowInBlock: Int = -1,
                        colInBlock: Long = -1,
                        rowType: RowType = RowType.T_DOUBLE_DENSE,
                        additionalConfiguration: Map[String, String] = Map()): MatrixMeta = {
    val maxRowNumInBlock = if (rowInBlock == -1) rows else rowInBlock
    val maxBlockSize = 5000000
    val maxColNumInBlock = if (colInBlock == -1) {
      math.max(maxBlockSize / maxRowNumInBlock, 1L)
    } else {
      colInBlock
    }
    createMatrix(rows, cols, -1, maxRowNumInBlock, maxColNumInBlock, rowType, additionalConfiguration)
  }


  def createMatrix(rows: Int,
                   cols: Long,
                   validIndexNum: Long,
                   rowInBlock: Int,
                   colInBlock: Long,
                   rowType: RowType,
                   additionalConfiguration: Map[String, String] = Map()): MatrixMeta = {
    assertCallByDriver("The operation of creating a matrix can only be called on the driver side.")
    val mc = new MatrixContext(s"spark-$matrixCounter", rows, cols, validIndexNum,
      rowInBlock, colInBlock, rowType)

    additionalConfiguration.foreach { case (key, value) =>
      mc.getAttributes.put(key, value)}

    if (mc.getAttributes.containsKey(AngelConf.Angel_PS_PARTITION_CLASS)) {
      val partitionClassName = mc.getAttributes.get(AngelConf.Angel_PS_PARTITION_CLASS)
      mc.setPartitionerClass(Class.forName(partitionClassName).asInstanceOf[Class[Partitioner]])
      mc.getAttributes.remove(AngelConf.Angel_PS_PARTITION_CLASS)
    }
    psAgent.createMatrix(mc, 5000L)
    val meta = psAgent.getMatrix(mc.getName)
    matrixCounter += 1
    matrixMetaMap(meta.getId) = meta
    meta
  }

  def getMatrixMeta(matrixId: Int): Option[MatrixMeta] = {
    matrixMetaMap.get(matrixId)
  }

  /**
    * create a sparse long key matrix
    *
    * @param rows          matrix row num
    * @param cols          dimension for each row
    * @param validIndexNum long type range for each row
    *                      if range = -1, the range is (Long.MinValue, Long.MaxValue),
    *                      else range is [0, range), usually range will be set to Long.MaxValue.
    * @param rowInBlock    row number for each block
    * @param colInBlock    col number for each block
    * @return
    */
  def createSparseMatrix(rows: Int,
                         cols: Long,
                         validIndexNum: Long,
                         rowInBlock: Int = -1,
                         colInBlock: Long = -1,
                         rowType: RowType = RowType.T_DOUBLE_SPARSE,
                         additionalConfiguration: Map[String, String] = Map()): MatrixMeta = {
    createMatrix(rows, cols, validIndexNum, rowInBlock, colInBlock, rowType, additionalConfiguration)
  }

  def duplicateVector(original: PSVector): PSVector = {
    assertCallByDriver("The operation of duplicate a vector can only be called on the driver side.")
    getPool(original.poolId).allocate().reset
  }

  def destroyVector(vector: PSVector): Unit = {
    assertCallByDriver("The operation of destroying a vector can only be called on the driver side.")
    getPool(vector.poolId).delete(vector)
  }

  private[spark] def getPool(id: Int): PSVectorPool = {
    psVectorPools.get(id)
  }

  def destroyVectorPool(vector: PSVector): Unit = {
    assertCallByDriver("The operation of destroying a matrix can only be called on the driver side.")
    destroyVectorPool(vector.poolId)
  }

  def destroyVectorPool(poolId: Int): Unit = {
    val pool = psVectorPools.remove(poolId)
    pool.destroy()
    // destroy matrix
    destroyMatrix(poolId)
  }

  def destroyMatrix(matrixId: Int): Unit = {
    matrixMetaMap.remove(matrixId).foreach { x =>
      psAgent.releaseMatrix(x.getId)
    }
  }

  /**
    * Refresh the matrix meta information for this psagent.
    * This method will pull matrix infos from master.
    */
  override def refreshMatrix(): Unit = {
    psAgent.refreshMatrixInfo()
  }

  def TOTAL_PS_CORES: Int = angelCtx.getConf.getInt(TOTAL_CORES, 2)

  def getMatrixClient(matrixId: Int): MatrixClient = {
    MatrixClientFactory.get(matrixId, getTaskId)
  }

  psAgent.initAndStart()

  ShutdownHookManager.get().addShutdownHook(
    new Runnable {
      def run(): Unit = psAgent.stop()
    },
    FileSystem.SHUTDOWN_HOOK_PRIORITY + 20
  )

  //========Init Part Finish======//

  private def getTaskId: Int = {
    val tc = TaskContext.get()
    if (tc == null) {
      -1
    } else {
      tc.partitionId()
    }
  }

  protected def stop() {
    matrixMetaMap.foreach { entry =>
      destroyMatrix(entry._1)
    }

    if (psAgent != null) {
      psAgent.stop()
    }
  }

  private[spark] def conf: Map[String, String] = angelConf
}

private[spark] object AngelPSContext {

  private val MASTER_IP = "angel.master.ip"
  private val MASTER_PORT = "angel.master.port"
  private val TOTAL_CORES = "angel.ps.total.cores"
  private val CONF_KEYS = "angel.conf.keys"

  private var angelClient: com.tencent.angel.client.AngelPSClient = _
  private var hookTask: Runnable = _

  def apply(executorId: String, sparkConf: SparkConf): AngelPSContext = {
    if (executorId == "driver") {
      val angelContext = launchAngel(sparkConf)
      new AngelPSContext(-1, angelContext)
    } else {
      val taskConf = SparkHadoopUtil.get.newConfiguration(sparkConf)

      val taskContext = TaskContext.get()
      val ip = taskContext.getLocalProperty(MASTER_IP)
      val port = taskContext.getLocalProperty(MASTER_PORT).toInt
      val masterAddr = new Location(ip, port)
      val keys = taskContext.getLocalProperty(CONF_KEYS).split(";")
      for (key <- keys) {
        taskConf.set(key, taskContext.getLocalProperty(key))
      }

      val angelContext = new AngelContext(masterAddr, taskConf)
      new AngelPSContext(executorId.toInt - 1, angelContext)
    }
  }

  def save(context: ModelSaveContext): Unit = {
    angelClient.save(context)
  }

  def load(context: ModelLoadContext): Unit = {
    angelClient.load(context)
  }

  //Start Angel
  private def launchAngel(conf: SparkConf): AngelContext = {
    angelClient = new com.tencent.angel.client.AngelPSClient(convertToHadoop(conf))
    hookTask = new Runnable {
      def run(): Unit = doStop()
    }
    ShutdownHookManager.get().addShutdownHook(hookTask,
      FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)

    angelClient.startPS()
  }

  /**
    * Convert SparkConf to Angel Configuration.
    */
  private def convertToHadoop(conf: SparkConf): Configuration = {
    val appName = conf.get("spark.app.name") + "-ps"
    val queue = conf.get("spark.yarn.queue", "root.default")

    /** mode: YARN or LOCAL */
    val master = conf.getOption("spark.master")
    val isLocal = if (master.isEmpty || master.get.toLowerCase.startsWith("local")) true else false
    val deployMode = if (isLocal) "LOCAL" else conf.get("spark.ps.mode", DEFAULT_ANGEL_DEPLOY_MODE)

    val psNum = conf.getInt("spark.ps.instances", 1)
    val psCores = conf.getInt("spark.ps.cores", 1)
    val psMem = conf.getSizeAsGb("spark.ps.memory", "4g").toInt
    val psOpts = conf.getOption("spark.ps.extraJavaOptions")

    val psJars = conf.get("spark.ps.jars", "")
    val psLogLevel = conf.get("spark.ps.log.level", "INFO")
    val psClass = conf.get("spark.ps.class", classOf[ParameterServer].getName)

    val defaultFS = conf.get("spark.hadoop.fs.defaultFS", "file://")
    val tempPath = defaultFS + "/tmp/spark-on-angel/" + UUID.randomUUID()

    val psOutOverwrite = conf.getBoolean("spark.ps.out.overwrite", defaultValue = true)
    val psOutTmpOption = conf.getOption("spark.ps.out.tmp.path.prefix")

    import com.tencent.angel.conf.AngelConf._

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

    // setting running mode, app name, queue and deploy mode
    hadoopConf.set(ANGEL_RUNNING_MODE, "ANGEL_PS")
    hadoopConf.set(ANGEL_JOB_NAME, appName)
    hadoopConf.set(ANGEL_QUEUE, queue)
    hadoopConf.set(ANGEL_DEPLOY_MODE, deployMode)

    // For local mode, we set heartbeat a small value for fast debugging
    if (deployMode == "LOCAL")
      hadoopConf.set(ANGEL_PS_HEARTBEAT_INTERVAL_MS, "200")

    if (psOpts.isDefined)
      hadoopConf.set(ANGEL_PS_JAVA_OPTS, psOpts.get)

    // Set the temp path as the angel.save.model.path to fake the angel-ps system
    // The action type is also a fake setting.
    hadoopConf.set(ANGEL_ACTION_TYPE, "train")
    hadoopConf.set(ANGEL_SAVE_MODEL_PATH, tempPath)

    // Setting resource
    hadoopConf.setInt(ANGEL_PS_NUMBER, psNum)
    hadoopConf.setInt(ANGEL_PS_CPU_VCORES, psCores)
    hadoopConf.setInt(ANGEL_PS_MEMORY_GB, psMem)
    hadoopConf.setInt(TOTAL_CORES, psNum * psCores)

    hadoopConf.set(ANGEL_AM_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_CLASS, psClass)
    hadoopConf.set(ANGEL_JOB_LIBJARS, psJars)

    hadoopConf.setBoolean(ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, psOutOverwrite)
    psOutTmpOption.foreach(hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_PATH_PREFIX, _))

    // No need for sync clock values, we don't need ssp in Spark.
    hadoopConf.setInt(ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100000000)
    hadoopConf.set(ANGEL_LOG_PATH, tempPath)

    // add user resource files
   addUserResourceFiles(conf, hadoopConf)

    // Some other settings
    conf.getAllWithPrefix("angel").foreach {
      case (key, value) => hadoopConf.set(s"angel$key", value)
    }
    hadoopConf
  }

  private def addUserResourceFiles(sparkConf: SparkConf, conf: Configuration): Unit = {
    val appStagingBaseDir = sparkConf.getOption("spark.yarn.stagingDir")
      .fold(FileSystem.get(conf).getHomeDirectory)(new Path(_))
    val appStagingDir = new Path(appStagingBaseDir, s".sparkStaging/${sparkConf.getAppId}")
    val resourceFiles = new ArrayBuffer[String]()
    sparkConf.getOption("spark.yarn.dist.files").foreach { fileList =>
      fileList.split(",").foreach { file =>
        val fileName = file.trim.split("/").last
        if (fileName.nonEmpty) {
          resourceFiles.append(new Path(appStagingDir, fileName).toString)
        }
      }
      conf.set(AngelConf.ANGEL_APP_USER_RESOURCE_FILES, resourceFiles.mkString(","))
    }
  }

  private def assertCallByDriver(message: String): Unit = {
    val executorId = SparkEnv.get.executorId
    if (executorId != "driver")
      throw new AngelException(s"executorId = $executorId, $message")
  }

  def stopAngel() {
    if (hookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(hookTask)
      hookTask = null
    }

    doStop()
  }

  def doStop(): Unit = {
    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }

  /**
    * return if AngelPSClient is alive
    */
  def isAlive: Boolean = {
    if (angelClient != null && hookTask != null) true else false
  }

  def adjustExecutorJVM(conf: SparkConf): SparkConf = {
    val extraOps = conf.getOption("spark.ps.executor.extraJavaOptions")
    val defaultOps = conf.get("spark.executor.extraJavaOptions", "")

    val extraOpsStr = if (extraOps.isDefined) {
      extraOps.get
    } else {
      var executorMemSizeInMB = conf.getSizeAsMb("spark.executor.memory", "2048M")
      if (executorMemSizeInMB < 2048) executorMemSizeInMB = 2048

      val isUseDirect: Boolean = conf.getBoolean("spark.ps.usedirectbuffer", defaultValue = true)
      val maxUse = executorMemSizeInMB - 512
      var directRegionSize: Int = 0
      if (isUseDirect) directRegionSize = (maxUse * 0.3).toInt
      else directRegionSize = (maxUse * 0.2).toInt
      val heapMax = maxUse - directRegionSize
      val youngRegionSize = (heapMax * 0.3).toInt
      val survivorRatio = 4

      conf.set("spark.executor.memory", heapMax + "M")

      val executorOps = new StringBuilder()
        .append(" -Xmn").append(youngRegionSize).append("M")
        .append(" -XX:MaxDirectMemorySize=").append(directRegionSize).append("M")
        .append(" -XX:SurvivorRatio=").append(survivorRatio)
        .append(" -XX:+AggressiveOpts")
        .append(" -XX:+UseLargePages")
        .append(" -XX:+UseConcMarkSweepGC")
        .append(" -XX:CMSInitiatingOccupancyFraction=50")
        .append(" -XX:+UseCMSInitiatingOccupancyOnly")
        .append(" -XX:+CMSScavengeBeforeRemark")
        .append(" -XX:+UseCMSCompactAtFullCollection")
        .append(" -verbose:gc")
        .append(" -XX:+PrintGCDateStamps")
        .append(" -XX:+PrintGCDetails")
        .append(" -XX:+PrintCommandLineFlags")
        .append(" -XX:+PrintTenuringDistribution")
        .append(" -XX:+PrintAdaptiveSizePolicy")
        .toString()
      executorOps
    }
    conf.set("spark.executor.extraJavaOptions", defaultOps + " " + extraOpsStr)
  }
}
