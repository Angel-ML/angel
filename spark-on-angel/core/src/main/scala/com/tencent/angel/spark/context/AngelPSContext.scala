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
 *
 */

package com.tencent.angel.spark.context

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, TaskContext}

import com.tencent.angel.client.AngelContext
import com.tencent.angel.common.Location
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta}
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.PSContext
import com.tencent.angel.spark.models.{BitSetPSModelPool, PSModelPool}

/**
 * AngelPSContext for driver and executor, it is an implement of `PSContext`
 */
private[spark] class AngelPSContext private (id: Int, angelCtx: AngelContext) extends PSContext {

  import AngelPSContext._

  private val totalPSCores = angelCtx.getConf.getInt(TOTAL_CORES, 2)

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

  private val matrixMetaMap = mutable.HashMap.empty[Int, MatrixMeta]

  private var matrixCounter = 0

  private val psAgent = {
    val masterIp = angelCtx.getMasterLocation.getIp
    val masterPort = angelCtx.getMasterLocation.getPort
    new PSAgent(angelCtx.getConf, masterIp, masterPort, id, false, null)
  }

  psAgent.initAndStart()

  ShutdownHookManager.get().addShutdownHook(
    new Runnable {
      def run(): Unit = psAgent.stop()
    },
    FileSystem.SHUTDOWN_HOOK_PRIORITY + 20)


  override private[spark] def conf: Map[String, String] = angelConf

  /**
   * Create PSVectorPool in Angel PS nodes.
   * PSVectorPool is a matrix in Angel PS nodes,
   * psAgent creates the matrix for AngelPSClient.
   */
  protected def doCreateModelPool(numDimensions: Int, capacity: Int): PSModelPool = {
    val maxRowNumInBlock = if (capacity > 1000) capacity / totalPSCores else capacity
    val maxColNumInBlock = if (numDimensions > 1000) {
      numDimensions / totalPSCores
    } else {
      numDimensions
    }
    val matrix = new MatrixContext(s"spark-$matrixCounter", capacity, numDimensions,
      maxRowNumInBlock, maxColNumInBlock)
    val meta = psAgent.createMatrix(matrix, 5000L)
    matrixCounter += 1
    matrixMetaMap(meta.getId) = meta
    new BitSetPSModelPool(meta.getId, numDimensions, capacity)
  }

  /**
   * Call `psAgent` to release matrix which is PSVectorPool in AngelPSClient
   *
   * @param pool The poll to be destroyed
   */
  protected def doDestroyVectorPool(pool: PSModelPool): Unit = {
    matrixMetaMap.remove(pool.id).foreach { x =>
      psAgent.releaseMatrix(x)
    }
    pool.destroy()
  }

  def getMatrixClient(poolId: Int): MatrixClient = {
    MatrixClientFactory.get(poolId, getTaskId())
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
      new AngelPSContext(-1, submit(sparkConf))
    } else {
      val taskContext = TaskContext.get()

      val ip = taskContext.getLocalProperty(MASTER_IP)
      val port = taskContext.getLocalProperty(MASTER_PORT).toInt
      val masterAddr = new Location(ip, port)

      val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
      val keys = taskContext.getLocalProperty(CONF_KEYS).split(";")
      for (key <- keys) {
        hadoopConf.set(key, taskContext.getLocalProperty(key))
      }

      val angelContext = new AngelContext(masterAddr, hadoopConf)
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
    val deployMode = conf.get("spark.ps.mode", AngelConfiguration.DEFAULT_ANGEL_DEPLOY_MODE)
    val psNum = conf.getInt("spark.ps.instances", 2)
    val psCores = conf.getInt("spark.ps.cores", 2)
    val psMem = conf.getSizeAsMb("spark.ps.memory", "4g").toInt
    val psHeap = psMem - 200
    val psOpts = s" -Xms${psHeap}M -Xmx${psHeap}M " +
      s"${conf.get("spark.ps.extraJavaOptions", "")}"

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

    import com.tencent.angel.conf.AngelConfiguration._

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    hadoopConf.set(ANGEL_RUNNING_MODE, "ANGEL_PS")

    hadoopConf.set(ANGEL_JOB_NAME, appName)
    hadoopConf.set(ANGEL_QUEUE, queue)

    hadoopConf.set(ANGEL_DEPLOY_MODE, deployMode)
    hadoopConf.setInt(ANGEL_PS_NUMBER, psNum)
    hadoopConf.setInt(ANGEL_PS_CPU_VCORES, psCores)
    hadoopConf.setInt(ANGEL_PS_MEMORY_MB, psMem)
    hadoopConf.set(ANGEL_PS_JAVA_OPTS, psOpts)
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

    hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_DIRECTORY, jobTempPath)
    hadoopConf.setBoolean(ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, psOutOverwrite)
    psOutTmpOption.foreach {
      hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_DIRECTORY, _)
    }

    hadoopConf
  }

  private def submit(conf: SparkConf): AngelContext = {
    angelClient = new com.tencent.angel.client.AngelPSClient(convertToHadoop(conf))
    hookTask = new Runnable {
      def run(): Unit = doStop()
    }
    ShutdownHookManager.get().addShutdownHook(hookTask,
      FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)

    angelClient.startPS()
  }

  /**
   * Stop AngelPSClient
   */
  def stop(): Unit = {
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

  private def doStop(): Unit = {
    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }
}

