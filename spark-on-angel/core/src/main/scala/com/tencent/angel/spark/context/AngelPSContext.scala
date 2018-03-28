/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.context

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, TaskContext}

import com.tencent.angel.client.AngelContext
import com.tencent.angel.common.location.Location
import com.tencent.angel.conf.AngelConf._
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.matrix.MatrixType
import com.tencent.angel.spark.models.matrix.MatrixType.MatrixType
import com.tencent.angel.spark.models.vector.VectorType.VectorType
import com.tencent.angel.spark.models.vector.{PSVector, VectorType}

/**
 * AngelPSContext for driver and executor, it is an implement of `PSContext`
 */
private[spark] class AngelPSContext(contextId: Int, angelCtx: AngelContext) extends PSContext {

  import AngelPSContext._

  private[spark] def conf: Map[String, String] = angelConf

  protected def stop() = {
    matrixMetaMap.foreach { entry =>
      destroyMatrix(entry._1)
    }

    if (psAgent != null) {
      psAgent.stop()
    }
  }

  def createDenseMatrix(rows: Int, cols: Long, rowInBlock: Int = -1, colInBlock: Long = -1): MatrixMeta = {
    val maxRowNumInBlock = if (rowInBlock == -1) rows else rowInBlock
    val maxBlockSize = 5000000
    val maxColNumInBlock = if (colInBlock == -1) {
      math.max(maxBlockSize / maxRowNumInBlock, 1L)
    } else {
      colInBlock
    }
    val mc = new MatrixContext(s"spark-$matrixCounter", rows, cols,
      maxRowNumInBlock, maxColNumInBlock)
    mc.setRowType(RowType.T_DOUBLE_DENSE)
    psAgent.createMatrix(mc, 5000L)
    val meta = psAgent.getMatrix(mc.getName)
    matrixCounter += 1
    matrixMetaMap(meta.getId) = meta
    meta
  }

  /**
   * create a sparse long key matrix
   * @param rows matrix row num
   * @param dim dimension for each row
   * @param range long type range for each row
   *              if range = -1, the range is (Long.MinValue, Long.MaxValue),
   *              else range is [0, range), usually range will be set to Long.MaxValue.
   * @param rowInBlock row number for each block
   * @param colInBlock col number for each block
   * @return
   */
  def createSparseMatrix(
      rows: Int,
      dim: Long,
      range: Long,
      rowInBlock: Int = -1,
      colInBlock: Long = -1): MatrixMeta = {

    val maxRowNumInBlock = if (rowInBlock == -1) rows else rowInBlock
    val maxColNumInBlock = -1
    val mc = new MatrixContext(s"spark-$matrixCounter", rows, range, dim, maxRowNumInBlock, maxColNumInBlock)
    mc.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY)
    psAgent.createMatrix(mc, 5000L)
    val meta = psAgent.getMatrix(mc.getName)
    matrixCounter += 1
    matrixMetaMap(meta.getId) = meta
    meta
  }


  def destroyMatrix(matrixId: Int): Unit = {
    matrixMetaMap.remove(matrixId).foreach { x =>
      psAgent.releaseMatrix(x.getId)
    }
  }

  def createVector(
      dimension: Long,
      t: VectorType = VectorType.DENSE,
      poolCapacity: Int = PSVectorPool.DEFAULT_POOL_CAPACITY,
      range: Long): PSVector = {

    val vector = createVectorPool(dimension, poolCapacity, t, range).allocate()
    PSClient.instance().vectorOps.fill(vector, 0.0)
    vector
  }

  def duplicateVector(original: PSVector): PSVector = {
    val vector = getPool(original.poolId).allocate()
    PSClient.instance().vectorOps.fill(vector, 0.0)
    vector
  }

  def destroyVector(vector: PSVector): Unit ={
    getPool(vector.poolId).delete(vector)
  }

  private[spark] def createVectorPool(
      dimension: Long,
      capacity: Int,
      t: VectorType,
      range: Long): PSVectorPool = {

    val thisCapacity = if (capacity > 0) capacity else PSVectorPool.DEFAULT_POOL_CAPACITY

    val matrixMeta = t match {
      case VectorType.DENSE => createDenseMatrix(thisCapacity, dimension)
      case VectorType.SPARSE => createSparseMatrix(thisCapacity, dimension, range)
    }

    val pool = new PSVectorPool(matrixMeta.getId, dimension, thisCapacity, t)
    psVectorPools.put(pool.id, pool)
    pool
  }

  def destroyVectorPool(poolId: Int): Unit = {
    val pool = psVectorPools.remove(poolId)
    pool.destroy()
    // destroy matrix
    destroyMatrix(poolId)
  }

  def destroyVectorPool(vector: PSVector): Unit = {
    destroyVectorPool(vector.poolId)
  }


  def TOTAL_PS_CORES = angelCtx.getConf.getInt(TOTAL_CORES, 2)
  private val matrixMetaMap = mutable.HashMap.empty[Int, MatrixMeta]
  private var matrixCounter = 0
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
  psAgent.initAndStart()

  ShutdownHookManager.get().addShutdownHook(
    new Runnable {
      def run() = psAgent.stop()
    },
    FileSystem.SHUTDOWN_HOOK_PRIORITY + 20
  )

  //========Init Part Finish======//

  def getMatrixClient(matrixId: Int): MatrixClient = {
    MatrixClientFactory.get(matrixId, getTaskId())
  }

  private[spark] def getPool(id: Int): PSVectorPool = {
    psVectorPools.get(id)
  }

  private def getTaskId(): Int = {
    val tc = TaskContext.get()
    if (tc == null) {
      -1
    } else {
      tc.partitionId()
    }
  }
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
    val psMem = conf.getSizeAsMb("spark.ps.memory", "4g").toInt
    val psHeap = psMem - 200
    val psOpts = conf.getOption("spark.ps.extraJavaOptions")

    val psJars = conf.get("spark.ps.jars", "")
    val psLogLevel = conf.get("spark.ps.log.level", "INFO")
    val psClass = conf.get("spark.ps.class",
      "com.tencent.angel.ps.impl.ParameterServer")

    val actionType = conf.get("spark.ps.action.type", "train")

    val defaultFS = conf.get("spark.hadoop.fs.defaultFS", "file://")
    val tempPath = defaultFS + "/tmp/spark-on-angel/" + UUID.randomUUID()
    val jobTempPath = "/tmp/angel"

    val psOut = conf.get("spark.ps.out.path", tempPath)
    val modelPath = conf.get("spark.ps.model.path", tempPath)
    val psOutOverwrite = conf.getBoolean("spark.ps.out.overwrite", true)
    val psOutTmpOption = conf.getOption("spark.ps.out.tmp.path")

    import com.tencent.angel.conf.AngelConf._

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    hadoopConf.set(ANGEL_RUNNING_MODE, "ANGEL_PS")

    hadoopConf.set(ANGEL_JOB_NAME, appName)
    hadoopConf.set(ANGEL_QUEUE, queue)

    hadoopConf.set(ANGEL_DEPLOY_MODE, deployMode)
    if (deployMode == "LOCAL") {
      hadoopConf.set(ANGEL_PS_HEARTBEAT_INTERVAL_MS, "200")
    }
    if (psOpts.isDefined) {
      hadoopConf.set(ANGEL_PS_JAVA_OPTS, psOpts.get)
    }

    hadoopConf.setInt(ANGEL_PS_NUMBER, psNum)
    hadoopConf.setInt(ANGEL_PS_CPU_VCORES, psCores)
    hadoopConf.setInt(ANGEL_PS_MEMORY_MB, psMem)
    hadoopConf.setInt(TOTAL_CORES, psNum * psCores)

    hadoopConf.set(ANGEL_AM_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_LOG_LEVEL, psLogLevel)

    hadoopConf.set(ANGEL_PS_CLASS, psClass)

    hadoopConf.set(ANGEL_JOB_LIBJARS, psJars)

    hadoopConf.set(ANGEL_ACTION_TYPE, actionType)
    if (actionType == "train") {
      hadoopConf.set(ANGEL_SAVE_MODEL_PATH, modelPath)
    } else {
      hadoopConf.set(ANGEL_PREDICT_PATH, psOut)
    }

    hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_PATH, jobTempPath)
    hadoopConf.setBoolean(ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, psOutOverwrite)
    psOutTmpOption.foreach {
      hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_PATH, _)
    }

    hadoopConf.setInt(ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 10000000)
    hadoopConf
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

  def stopAngel() = {
    if (hookTask != null) {
      ShutdownHookManager.get().removeShutdownHook(hookTask)
      hookTask = null
    }

    doStop()
  }

  /**
   * return if AngelPSClient is alive
   */
  def isAlive: Boolean = {
    if (angelClient != null && hookTask != null) true else false
  }

  def doStop(): Unit = {
    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }

  def adjustExecutorJVM(conf: SparkConf) = {
    val extraOps = conf.getOption("spark.ps.executor.extraJavaOptions")
    val defaultOps = conf.get("spark.executor.extraJavaOptions", "")

    val extraOpsStr = if (extraOps.isDefined) {
      extraOps.get
    } else {
      var executorMemSizeInMB = conf.getSizeAsMb("spark.executor.memory", "2048M")
      if (executorMemSizeInMB < 2048) executorMemSizeInMB = 2048

      val isUseDirect: Boolean = conf.getBoolean("spark.ps.usedirectbuffer", true)
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
