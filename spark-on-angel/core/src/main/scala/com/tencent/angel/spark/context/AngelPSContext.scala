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
import org.apache.spark.AngelSparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}
import com.tencent.angel.ps.storage.partitioner.Partitioner

/**
  * AngelPSContext for driver and executor, it is an implement of `PSContext`
  */
class AngelPSContext(contextId: Int, angelCtx: AngelContext) extends PSContext {

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
  
  def getAngelConf: Configuration = {
    val angelConf = angelCtx.getConf
    angelConf.set(MASTER_IP, angelCtx.getMasterLocation.getIp)
    angelConf.set(MASTER_PORT, angelCtx.getMasterLocation.getPort.toString)
    angelConf
  }
  
  def getAngelClient(): com.tencent.angel.client.AngelClient = {
    if (angelClient != null) {
      angelClient.getClient
    } else if (attachClient != null) {
      attachClient
    } else {
      throw new AngelException(
        "AngelClient is not available: neither a driver-launched AngelPSClient nor an " +
          "attach-mode AngelAttachClient has been initialised. " +
          "Call AngelPSContext(sparkConf) on the driver before using save/load/etc.")
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

  def conf: Map[String, String] = angelConf

  override def createMatrix(mc: MatrixContext): MatrixMeta = {
    //assertCallByDriver("The operation of creating a matrix can only be called on the driver side.")
    psAgent.createMatrix(mc, 5000L)
    val meta = psAgent.getMatrix(mc.getName)
    matrixCounter += 1
    matrixMetaMap(meta.getId) = meta
    meta
  }

  def getPSAgent(): PSAgent = {
    psAgent
  }

  override def save(ctx: ModelSaveContext): Unit = AngelPSContext.save(ctx)

  override def checkpoint(checkpointId:Int, ctx: ModelSaveContext): Unit = AngelPSContext.checkpoint(checkpointId, ctx)

  override def load(ctx: ModelLoadContext): Unit = AngelPSContext.load(ctx)

  override def recover(checkpointId:Int, ctx: ModelLoadContext): Unit = AngelPSContext.recover(checkpointId, ctx)

  override def waitPS(): Unit = AngelPSContext.waitPS()
}

object AngelPSContext {

  private val MASTER_IP = "angel.master.ip"
  private val MASTER_PORT = "angel.master.port"
  private val TOTAL_CORES = "angel.ps.total.cores"
  private val CONF_KEYS = "angel.conf.keys"

  private var angelClient: com.tencent.angel.client.AngelPSClient = _
  // Client used when Spark attaches to an externally-launched Angel PS cluster. It is
  // functionally interchangeable with `angelClient` for save / load / checkpoint / recover
  // / waitAllPS, but does NOT own the remote cluster's lifecycle (stop() is local-only).
  private var attachClient: com.tencent.angel.client.attach.AngelAttachClient = _
  private var hookTask: Runnable = _

  def apply(executorId: String, sparkConf: SparkConf): AngelPSContext = {
    if (executorId == "driver") {
      val attachAddr = externalMasterAddr(sparkConf)
      if (attachAddr.isDefined) {
        println(s"Attach to pre-launched Angel PS cluster at ${attachAddr.get}")
        new AngelPSContext(-1, attachAngel(sparkConf, attachAddr.get))
      } else {
        println("from driver start Angel PS! ")
        val angelContext = launchAngel(sparkConf)
        new AngelPSContext(-1, angelContext)
      }
    } else {
      val taskConf = AngelSparkHadoopUtil.getConfiguration(sparkConf)
      println("from executor to connect Angel PS! ")
      // ----------------------------------------------------------------------------------
      // Resolution order for the master address on the executor side:
      //
      //   1. TaskContext.getLocalProperty(MASTER_IP / MASTER_PORT / CONF_KEYS)
      //      Spark sets these on the THREAD running the task via ThreadLocal. Works for
      //      code that runs directly inside Task.run() / iterator.next() / closure body.
      //
      //   2. sparkConf  (spark.angel.ps.master  /  spark.hadoop.angel.master.host+port)
      //      Used as a fallback when (1) is unavailable. Required when PSContext.instance()
      //      is invoked from a thread OTHER than the Spark task thread -- e.g.
      //      `BackpressureStage`'s own ExecutorService (HGATPartition.sampleUnsafe ->
      //      SampleAndFeatureUnsafe.process), which spawns worker threads for which
      //      TaskContext.get() returns null and getLocalProperty() would NPE.
      //
      //      The attach-mode driver already sets `spark.angel.ps.master` via
      //      --conf, so the same value is shipped to every executor in sparkConf. CONF_KEYS
      //      passthrough is skipped in this branch (the driver pre-populates everything
      //      relevant in spark.hadoop.* form, which Spark mirrors into the Hadoop conf).
      // ----------------------------------------------------------------------------------
      val taskContext = TaskContext.get()
      var ip: String = null
      var portStr: String = null
      var keysStr: String = null
      if (taskContext != null) {
        ip = taskContext.getLocalProperty(MASTER_IP)
        portStr = taskContext.getLocalProperty(MASTER_PORT)
        keysStr = taskContext.getLocalProperty(CONF_KEYS)
      }

      // Fallback path: TaskContext is null (background pool thread) OR a property is
      // missing. Reach into sparkConf for the externally-published master address.
      if (ip == null || portStr == null) {
        externalMasterAddr(sparkConf) match {
          case Some(loc) =>
            if (taskContext == null) {
              println(s"Executor $executorId: TaskContext.get() == null (likely a non-Spark " +
                s"worker thread, e.g. BackpressureStage); falling back to sparkConf " +
                s"spark.angel.ps.master=${loc.getIp}:${loc.getPort}")
            } else {
              println(s"Executor $executorId: TaskContext local properties incomplete " +
                s"(MASTER_IP=$ip, MASTER_PORT=$portStr); falling back to sparkConf " +
                s"spark.angel.ps.master=${loc.getIp}:${loc.getPort}")
            }
            ip = loc.getIp
            portStr = loc.getPort.toString
          case None =>
            throw new AngelException(
              s"Executor $executorId cannot init AngelPSContext: TaskContext local " +
                s"properties unavailable (MASTER_IP=$ip, MASTER_PORT=$portStr) AND " +
                s"sparkConf has no spark.angel.ps.master / spark.hadoop.angel.master.host. " +
                s"Set --conf spark.angel.ps.master=<host:port> when submitting the job, " +
                s"OR make sure PSContext.submit() runs on the driver before this " +
                s"executor task starts.")
        }
      }

      val masterAddr = new Location(ip, portStr.toInt)
      // CONF_KEYS is best-effort: when present (Spark task thread path) it lets the
      // driver push arbitrary "angel.*" knobs to the executor's Hadoop conf. When
      // absent (fallback path) the executor relies on whatever sparkConf already
      // carries, which is sufficient for an attach-mode client.
      if (keysStr != null) {
        val keys = keysStr.split(";")
        for (key <- keys) {
          if (taskContext != null) {
            val v = taskContext.getLocalProperty(key)
            if (v != null) taskConf.set(key, v)
          }
        }
      }

      val angelContext = new AngelContext(masterAddr, taskConf)
      new AngelPSContext(executorId.toInt - 1, angelContext)
    }
  }

  /**
   * Read either {@code spark.angel.ps.master} or the equivalent {@code spark.hadoop.angel.master.host}
   * /port pair from the SparkConf. When set, the driver will <b>not</b> launch a new Angel
   * cluster; instead it attaches to a pre-launched standalone Angel PS cluster (typically
   * brought up by `bin/angel-cluster-up`).
   *
   * Accepted forms:
   *   - {@code spark.angel.ps.master = host:port}
   *   - {@code spark.hadoop.angel.master.host = host} +
   *     {@code spark.hadoop.angel.master.port = port}
   */
  private def externalMasterAddr(conf: SparkConf): Option[Location] = {
    conf.getOption("spark.angel.ps.master").map(_.trim).filter(_.nonEmpty).map { hp =>
      val idx = hp.lastIndexOf(':')
      if (idx <= 0 || idx == hp.length - 1) {
        throw new AngelException(
          s"spark.angel.ps.master must look like 'host:port', got: $hp")
      }
      new Location(hp.substring(0, idx), hp.substring(idx + 1).toInt)
    }.orElse {
      val host = conf.getOption("spark.hadoop.angel.master.host")
        .orElse(conf.getOption("spark.hadoop.angel.am.host"))
      val port = conf.getOption("spark.hadoop.angel.master.port")
      (host, port) match {
        case (Some(h), Some(p)) if h.nonEmpty && p.nonEmpty => Some(new Location(h, p.toInt))
        case _ => None
      }
    }
  }

  /**
   * Build an {@link AngelContext} that points to an externally-managed master without
   * starting any AngelPSClient / AM / PS on the driver. The driver is registered as an
   * {@link com.tencent.angel.client.attach.AngelAttachClient} so that control APIs such
   * as {@link #save}, {@link #load}, {@link #checkpoint}, {@link #recover} and
   * {@link #waitAllPSReady} work identically to the {@code launchAngel} code path — the
   * only operational difference being that {@link #stopAngel} does NOT tear the cluster
   * down (the external launcher owns that lifecycle).
   */
  private def attachAngel(sparkConf: SparkConf, master: Location): AngelContext = {
    val hadoopConf = convertToHadoop(sparkConf)
    // These keys are propagated to executors via `angelConf` below; setting them here keeps
    // the executor-side init code path identical regardless of attach vs launch.
    hadoopConf.set(MASTER_IP, master.getIp)
    hadoopConf.set(MASTER_PORT, master.getPort.toString)

    // Register a driver-side AngelAttachClient that connects to the pre-launched master.
    // Any failure here is fatal: the caller explicitly asked to attach.
    attachClient =
      new com.tencent.angel.client.attach.AngelAttachClient(hadoopConf, master)
    attachClient.startPSServer()

    // Mirror the shutdown-hook behaviour of launchAngel so the heartbeat thread / RPC
    // connections are cleaned up when the driver JVM exits. Unlike launchAngel, doStop()
    // will not issue a StopRequest to the remote master (see AngelAttachClient.stop).
    hookTask = new Runnable {
      def run(): Unit = doStop()
    }
    ShutdownHookManager.get().addShutdownHook(hookTask,
      FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)

    new AngelContext(master, hadoopConf)
  }


  def getPsApplicationId(): String = {
    if (angelClient != null) angelClient.getAppId
    else if (attachClient != null) attachClient.getAppIdString
    else "external-angel-ps"
  }


  def save(context: ModelSaveContext): Unit = {
    liveClient("save").save(context)
  }

  def checkpoint(checkpointId:Int, context: ModelSaveContext): Unit = {
    liveClient("checkpoint").checkpoint(checkpointId, context)
  }

  def load(context: ModelLoadContext): Unit = {
    liveClient("load").load(context)
  }

  def recover(checkpointId:Int, context: ModelLoadContext): Unit = {
    liveClient("recover").recover(checkpointId, context)
  }

  def waitAllPSReady() = {
    if (angelClient != null) {
      angelClient.waitAllPSReady()
    } else if (attachClient != null) {
      val psNum = attachClient.getConf.getInt(
        AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER)
      attachClient.waitForAllPS(psNum)
    } else {
      throw new AngelException(
        "Cannot perform 'waitAllPSReady': no driver-side Angel client has been initialised. " +
          "Call AngelPSContext(sparkConf) on the driver first.")
    }
  }

  /**
   * Returns either the driver-launched {@link com.tencent.angel.client.AngelPSClient} or,
   * in attach mode, a thin shim wrapping {@link com.tencent.angel.client.attach.AngelAttachClient}
   * that exposes the same lifecycle-operation signatures used by callers (save/load/
   * checkpoint/recover/createMatrices/...).
   *
   * <p>The returned instance must NOT be used to call {@code stopPS()} / {@code killPS()}
   * in attach mode: the cluster is owned by the external launcher.
   */
  private def liveClient(op: String): LifecycleOps = {
    if (angelClient != null) {
      OwnedClient(angelClient)
    } else if (attachClient != null) {
      AttachedClient(attachClient)
    } else {
      throw new AngelException(
        s"Cannot perform '$op': no driver-side Angel client has been initialised. " +
          "Either launch Angel from Spark (default) or attach to a pre-launched cluster " +
          "via spark.angel.ps.master=host:port.")
    }
  }

  /**
   * Minimal operation surface shared by the launch-owned {@link com.tencent.angel.client.AngelPSClient}
   * and the {@link com.tencent.angel.client.attach.AngelAttachClient} used in attach mode.
   * Both route identical RPCs to the same master; this shim only exists because the two
   * concrete types do not share an interface upstream.
   */
  private sealed trait LifecycleOps {
    def save(ctx: ModelSaveContext): Unit
    def load(ctx: ModelLoadContext): Unit
    def checkpoint(checkpointId: Int, ctx: ModelSaveContext): Unit
    def recover(checkpointId: Int, ctx: ModelLoadContext): Unit
  }

  private case class OwnedClient(c: com.tencent.angel.client.AngelPSClient) extends LifecycleOps {
    override def save(ctx: ModelSaveContext): Unit = c.save(ctx)
    override def load(ctx: ModelLoadContext): Unit = c.load(ctx)
    override def checkpoint(checkpointId: Int, ctx: ModelSaveContext): Unit =
      c.checkpoint(checkpointId, ctx)
    override def recover(checkpointId: Int, ctx: ModelLoadContext): Unit =
      c.recover(checkpointId, ctx)
  }

  private case class AttachedClient(c: com.tencent.angel.client.attach.AngelAttachClient)
      extends LifecycleOps {
    override def save(ctx: ModelSaveContext): Unit = c.save(ctx)
    override def load(ctx: ModelLoadContext): Unit = c.load(ctx)
    override def checkpoint(checkpointId: Int, ctx: ModelSaveContext): Unit =
      c.checkpoint(checkpointId, ctx)
    override def recover(checkpointId: Int, ctx: ModelLoadContext): Unit =
      c.recover(checkpointId, ctx)
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
  def convertToHadoop(conf: SparkConf): Configuration = {
    val appName = conf.get("spark.app.name") + "-ps"
    val queue = conf.get("spark.yarn.queue", "root.default")

    /** mode: LOCAL, YARN, EMBEDDED, STANDALONE or KUBERNETES */
    val master = conf.getOption("spark.master")
    val isLocal = if (master.isEmpty || master.get.toLowerCase.startsWith("local")) true else false
    val confDeployMode = conf.get("spark.ps.mode", DEFAULT_ANGEL_DEPLOY_MODE)
    val deployMode = if (isLocal && !confDeployMode.equalsIgnoreCase("EMBEDDED")) "LOCAL" else confDeployMode
    val masterMem = conf.getSizeAsGb("spark.angel.master.memory", "2g").toInt
    val psNum = conf.getInt("spark.ps.instances", 1)
    val psCores = conf.getInt("spark.ps.cores", 1)
    val psMem = conf.getSizeAsGb("spark.ps.memory", "4g").toInt
    val psOpts = conf.getOption("spark.ps.extraJavaOptions")

    val psJars = conf.get("spark.ps.jars", "")
    val psLogLevel = conf.get("spark.ps.log.level", "INFO")
    val psClass = conf.get("spark.ps.class", classOf[ParameterServer].getName)

    val hadoopDefaultFS = SparkContext.getOrCreate().hadoopConfiguration.get("fs.defaultFS", "file://")
    val defaultFS = conf.get("spark.hadoop.fs.defaultFS", hadoopDefaultFS)
    val tempPath = defaultFS + "/tmp/spark-on-angel/" + UUID.randomUUID()

    val psOutOverwrite = conf.getBoolean("spark.ps.out.overwrite", defaultValue = true)
    val psOutTmpOption = conf.getOption("spark.ps.out.tmp.path.prefix")

    import com.tencent.angel.conf.AngelConf._
    
    val hadoopConf = AngelSparkHadoopUtil.getConfiguration(conf)

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

    if (deployMode == "KUBERNETES") {
      hadoopConf.set(ANGEL_KUBERNETES_MASTER, master.get.substring("k8s://".length))
    }

    hadoopConf.setInt(ANGEL_AM_MEMORY_GB, masterMem)

    // Setting resource. spark.ps.instances feeds the driver-launched ANGEL_PS_LAUNCH path
    // (where Spark itself is the supervisor and tells Angel "I want N PSes"). When we are
    // ATTACHING to a pre-launched cluster the user has typically pinned the real PS count
    // via --conf spark.hadoop.angel.ps.number=<N> already (which SparkHadoopUtil has just
    // copied into hadoopConf). Honour that explicit value over the unrelated 'spark.ps
    // .instances' default; without this the driver always sees psNumber=1 and fights the
    // master-side count, eventually hanging in waitForAllPS forever.
    val explicitPsNum = conf.getOption("spark.hadoop." + ANGEL_PS_NUMBER)
                            .orElse(Option(hadoopConf.get(ANGEL_PS_NUMBER)))
                            .map(_.toInt)
    val effectivePsNum = explicitPsNum.getOrElse(psNum)
    hadoopConf.setInt(ANGEL_PS_NUMBER, effectivePsNum)
    hadoopConf.setInt(ANGEL_PS_CPU_VCORES, psCores)
    hadoopConf.setInt(ANGEL_PS_MEMORY_GB, psMem)
    hadoopConf.setInt(TOTAL_CORES, effectivePsNum * psCores)

    hadoopConf.set(ANGEL_AM_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_CLASS, psClass)
    hadoopConf.set(ANGEL_JOB_LIBJARS, psJars)

    hadoopConf.setBoolean(ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, psOutOverwrite)
    psOutTmpOption.foreach(hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_PATH_PREFIX, _))

    // No need for sync clock values, we don't need ssp in Spark.
    hadoopConf.setInt(ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100000000)
    hadoopConf.set(ANGEL_LOG_PATH, tempPath)

    hadoopConf.set(ANGEL_APPLICATION_ID, conf.getAppId)

    // add user resource files
    if (deployMode != "KUBERNETES") {
      addUserResourceFiles(conf, hadoopConf)
    }

    // Some other settings
    conf.getAllWithPrefix("angel").foreach {
      case (key, value) => hadoopConf.set(s"angel$key", value)
    }
    conf.getAllWithPrefix("spark.angel").foreach {
      case (key, value) => hadoopConf.set(s"angel$key", value)
    }
    
    if(deployMode.equalsIgnoreCase("EMBEDDED")) {
      hadoopConf.setBoolean(ANGEL_PS_REQUEST_RESOURCE_USE_MINIBATCH, false)
    }
    
    if(isLocal && deployMode.equalsIgnoreCase("EMBEDDED")) {
      // Local and embedded mode
      val newConf = new Configuration()
      val iter = hadoopConf.iterator()
      while(iter.hasNext) {
        val confEntry = iter.next()
        if(confEntry.getKey.startsWith("angel")) {
          newConf.set(confEntry.getKey, confEntry.getValue)
        }
      }
      newConf
    } else {
      hadoopConf
    }
  }

  private def addUserResourceFiles(sparkConf: SparkConf, conf: Configuration): Unit = {
    val appStagingBaseDir = sparkConf.getOption("spark.yarn.stagingDir")
      .fold(FileSystem.get(conf).getHomeDirectory)(new Path(_))
    val appStagingDir = new Path(appStagingBaseDir, s".sparkStaging/${sparkConf.getAppId}")

    // Pass the Spark staging dir to Angel so that AngelYarnClient can resolve bare filenames
    conf.set(AngelConf.ANGEL_APP_SPARK_STAGING_DIR, appStagingDir.toString)

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
    if (attachClient != null) {
      // Detach only; never stops the externally-launched cluster (see AngelAttachClient.stop).
      try {
        attachClient.stop()
      } catch {
        case t: Throwable =>
          // Detaching is best-effort: a remote master may already be gone. Failure here
          // must not prevent the Spark driver shutdown sequence from progressing.
          println("Detach from external Angel master failed: " + t.getMessage)
      }
      attachClient = null
    }
  }

  /**
    * Return whether a driver-side Angel client is wired up. In <b>launch mode</b> this
    * means the {@link com.tencent.angel.client.AngelPSClient} owns the cluster and the
    * shutdown hook is installed. In <b>attach mode</b> it means the driver has an active
    * {@link com.tencent.angel.client.attach.AngelAttachClient} pointing at the external
    * master; the actual liveness of that cluster is managed by whoever started it.
    */
  def isAlive: Boolean = {
    if (angelClient != null && hookTask != null) true
    else if (attachClient != null) true
    else false
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

  /**
   * Wait until all PS processes have registered with the master. Works both when the
   * driver launched Angel itself and when Spark is attached to a pre-launched cluster.
   *
   * In the STANDALONE / external-PS attach scenario the cluster bootstrap already gates
   * on every PS having logged "Register to AppMaster successfully" before submitting
   * spark-submit, so calling [[AngelClient.waitForAllPS]] from the driver is redundant
   * and — worse — hangs forever if the driver-side `angel.ps.number` does not exactly
   * match the master-side count (strict equality check in waitForAllPS). Operators can
   * therefore short-circuit the wait by setting:
   *
   *   --conf spark.hadoop.angel.ps.attach.skip.wait=true
   *
   * The default is `false` so existing deployments keep their behaviour.
   */
  def waitPS(): Unit = {
    if (angelClient != null) {
      angelClient.waitPS()
    } else if (attachClient != null) {
      val skipWait = attachClient.getConf.getBoolean(
        AngelConf.ANGEL_PS_ATTACH_SKIP_WAIT, AngelConf.DEFAULT_ANGEL_PS_ATTACH_SKIP_WAIT)
      if (skipWait) {
        PSContext.LOG.info(s"${AngelConf.ANGEL_PS_ATTACH_SKIP_WAIT}=true; skipping " +
          "AngelClient.waitForAllPS — caller asserts every PS has already registered " +
          "with the master before this driver started.")
        return
      }
      val psNum = attachClient.getConf.getInt(
        AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER)
      try {
        attachClient.waitForAllPS(psNum)
      } catch {
        case t: Throwable =>
          throw new AngelException("waitPS failed against external Angel master", t)
      }
    }
  }
}
