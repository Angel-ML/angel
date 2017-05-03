package com.tencent.angel.spark.client

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil

import com.tencent.angel.client.AngelContext
import com.tencent.angel.common.Location
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta}
import com.tencent.angel.ml.matrix.udf.aggr.{AggrFunc, AggrResult}
import com.tencent.angel.ml.matrix.udf.getrow.{DefaultGetRowFunc, GetRowParam}
import com.tencent.angel.ml.matrix.udf.updater.UpdaterFunc
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark._
import com.tencent.angel.spark.func._
import com.tencent.angel.spark.func.dist.aggr._
import com.tencent.angel.spark.func.dist.updater
import com.tencent.angel.spark.func.dist.updater._
import com.tencent.angel.spark.pool.BitSetPSVectorPool
import com.tencent.angel.spark.vector.RemotePSVector

/**
 * AngelPSClient is a Angel PS implement of PSClient. AngelPSClient builds a bridge between
 * Spark-on-Angel and Angel, AngelPSClient implements all `PSClient` `do*`s operations by calling
 * Angel's operations.
 */
private[spark] class AngelPSClient private(id: Int, angelCtx: AngelContext)
  extends PSClient {

  import AngelPSClient._

  private val totalPSCores = angelCtx.getConf.getInt(TOTAL_CORES, 2)

  /**
   * The mergeCache is used for increment/mergeMax/mergeMin of [[RemotePSVector]].
   * Increment/mergeMax/mergeMin is called on the Spark Executors. In order to reduce the times of
   * updating PS nodes, executors merge the update Vectors in local and flush the merged result to
   * PS nodes after processing all data in one executor.
   */
  private val mergeCache = new ConcurrentHashMap[Int, ArrayBuffer[RemotePSVector]]()

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

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new SparkException("PS computation failed!")
    }
  }

  private[spark] override def getAngelConf(): Map[String, String] = angelConf

  private[spark] override def register(vector: PSVector): Unit = {
    require(vector.isInstanceOf[RemotePSVector], "vector can only be RemotePSVector")
    val taskId = getTaskId()
    if (!mergeCache.contains(taskId)) {
      mergeCache.put(taskId, ArrayBuffer.empty)
    }
    mergeCache.get(taskId) += vector.asInstanceOf[RemotePSVector]
  }

  /**
   * Create PSVectorPool in Angel PS nodes.
   * PSVectorPool is a matrix in Angel PS nodes,
   * psAgent creates the matrix for AngelPSClient.
   */
  protected def doCreateVectorPool(numDimensions: Int, capacity: Int): PSVectorPool = {
    val matrix = new MatrixContext(s"spark-$matrixCounter", capacity, numDimensions,
      capacity, math.max(numDimensions / totalPSCores + 1, 1000))
    val meta = psAgent.createMatrix(matrix, 5000L)
    matrixCounter += 1
    matrixMetaMap(meta.getId) = meta
    new BitSetPSVectorPool(this, meta.getId, numDimensions, capacity)
  }

  /**
   * Call `psAgent` to release matrix which is PSVectorPool in AngelPSClient
   *
   * @param pool The poll to be destoried
   */
  protected def doDestroyVectorPool(pool: PSVectorPool): Unit = {
    matrixMetaMap.remove(pool.id).foreach { x =>
      psAgent.releaseMatrix(x)
    }
    pool.destroy()
  }

  protected def doPut(to: PSVectorProxy, value: Array[Double]): Unit = {
    update(to.poolId, new Put(to.poolId, to.id, value))
  }

  protected def doFill(to: PSVectorProxy, value: Double): Unit = {
    update(to.poolId, new Fill(to.poolId, to.id, value))
  }

  protected def doRandomUniform(
      to: PSVectorProxy,
      min: Double,
      max: Double): Unit = {
    update(to.poolId, new RandomUniform(to.poolId, to.id, min, max))
  }

  protected def doRandomNormal(
      to: PSVectorProxy,
      mean: Double,
      stddev: Double): Unit = {
    update(to.poolId, new RandomNormal(to.poolId, to.id, mean, stddev))
  }

  protected def doSum(vector: PSVectorProxy): Double = {
    aggregate(vector.poolId, new Sum(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doMax(vector: PSVectorProxy): Double = {
    aggregate(vector.poolId, new Max(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doMin(vector: PSVectorProxy): Double = {
    aggregate(vector.poolId, new Min(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doNnz(vector: PSVectorProxy): Int = {
    aggregate(vector.poolId, new Nnz(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toInt
  }

  protected def doAdd(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    update(from.poolId, new AddS(from.poolId, from.id, to.id, value))
  }

  protected def doMul(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    update(from.poolId, new MulS(from.poolId, from.id, to.id, value))
  }

  protected def doDiv(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    update(from.poolId, new DivS(from.poolId, from.id, to.id, value))
  }

  protected def doPow(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    update(from.poolId, new Pow(from.poolId, from.id, to.id, value))
  }

  protected def doSqrt(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Sqrt(from.poolId, from.id, to.id))
  }

  protected def doExp(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Exp(from.poolId, from.id, to.id))
  }

  protected def doExpm1(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Expm1(from.poolId, from.id, to.id))
  }

  protected def doLog(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Log(from.poolId, from.id, to.id))
  }

  protected def doLog1p(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Log1p(from.poolId, from.id, to.id))
  }

  protected def doLog10(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Log10(from.poolId, from.id, to.id))
  }

  protected def doCeil(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Ceil(from.poolId, from.id, to.id))
  }

  protected def doFloor(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Floor(from.poolId, from.id, to.id))
  }

  protected def doRound(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Round(from.poolId, from.id, to.id))
  }

  protected def doAbs(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Abs(from.poolId, from.id, to.id))
  }

  protected def doSignum(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    update(from.poolId, new Signum(from.poolId, from.id, to.id))
  }

  protected def doAdd(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new Add(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doSub(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new Sub(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMul(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new Mul(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doDiv(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new Div(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMax(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new MaxV(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMin(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new MinV(from1.poolId, from1.id, from2.id, to.id))
  }

  protected def doMap(
      from: PSVectorProxy,
      func: MapFunc,
      to: PSVectorProxy): Unit = {
    update(from.poolId, new updater.Map(from.poolId, from.id, to.id, func))
  }

  protected def doZip2Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapFunc,
      to: PSVectorProxy): Unit = {
    update(from1.poolId, new Zip2Map(from1.poolId, from1.id, from2.id, to.id, func))
  }

  protected def doZip3Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapFunc,
      to: PSVectorProxy): Unit = {
    update(from1.poolId,
      new Zip3Map(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  protected def doMapWithIndex(
      from: PSVectorProxy,
      func: MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    update(from.poolId, new MapWithIndex(from.poolId, from.id, to.id, func))
  }

  protected def doZip2MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    update(from1.poolId,
      new Zip2MapWithIndex(from1.poolId, from1.id, from2.id, to.id, func))
  }

  protected def doZip3MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    update(from1.poolId,
      new Zip3MapWithIndex(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  protected def doAxpy(a: Double, x: PSVectorProxy, y: PSVectorProxy): Unit = {
    update(x.poolId, new Axpy(x.poolId, x.id, y.id, a))
  }

  protected def doDot(x: PSVectorProxy, y: PSVectorProxy): Double = {
    aggregate(x.poolId, new Dot(x.poolId, x.id, y.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doCopy(x: PSVectorProxy, y: PSVectorProxy): Unit = {
    update(x.poolId, new Copy(x.poolId, x.id, y.id))
  }

  protected def doScal(a: Double, x: PSVectorProxy): Unit = {
    update(x.poolId, new Scale(x.poolId, x.id, a))
  }

  protected def doNrm2(x: PSVectorProxy): Double = {
    aggregate(x.poolId, new Nrm2(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAsum(x: PSVectorProxy): Double = {
    aggregate(x.poolId, new Asum(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAmax(x: PSVectorProxy): Double = {
    aggregate(x.poolId, new Amax(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doAmin(x: PSVectorProxy): Double = {
    aggregate(x.poolId, new Amin(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  protected def doGet(vector: PSVectorProxy): Array[Double] = {
    val client = MatrixClientFactory.get(vector.poolId, getTaskId())
    val param = new GetRowParam(vector.poolId, vector.id, 0, true)
    val func = new DefaultGetRowFunc(param)
    client.getRow(func).getRow.asInstanceOf[DenseDoubleVector].getValues
  }

  protected def doIncrement(vector: PSVectorProxy, delta: Array[Double]): Unit = {
    update(vector.poolId, new Increment(vector.poolId, vector.id, delta))
  }

  protected def doMergeMax(vector: PSVectorProxy, other: Array[Double]): Unit = {
    update(vector.poolId, new MaxA(vector.poolId, vector.id, other))
  }

  protected def doMergeMin(vector: PSVectorProxy, other: Array[Double]): Unit = {
    update(vector.poolId, new MinA(vector.poolId, vector.id, other))
  }

  override protected def doFlush(): Unit = {
    val taskId = getTaskId()
    if (mergeCache.containsKey(taskId)) {
      val buffer = mergeCache.get(taskId)
      buffer.foreach(_.flush())
      buffer.clear()
    }
  }

  /**
   * Use PS Oriented Function(POF) to update a PSVector on PS nodes.
   *
   * @param poolId The PSVectorPool id(matrix id in Angel)
   * @param func update PS Oriented Function(POF)
   */
  private def update(poolId: Int, func: UpdaterFunc): Unit = {
    val client = MatrixClientFactory.get(poolId, getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  /**
   * Use PS Oriented Function(POF) to aggregate a PSVector on PS nodes.
   *
   * @param poolId PSVectorPool id(matrix id in Angel)
   * @param func aggregate PS Oriented Function(POF)
   * @return the result of aggregate function
   */
  private def aggregate(poolId: Int, func: AggrFunc): AggrResult = {
    val client = MatrixClientFactory.get(poolId, getTaskId())
    val result = client.aggr(func)
    assertSuccess(result)
    result
  }
}

private[spark] object AngelPSClient {

  private val MASTER_IP = "angel.master.ip"
  private val MASTER_PORT = "angel.master.port"
  private val TOTAL_CORES = "angel.ps.total.cores"
  private val CONF_KEYS = "angel.conf.keys"

  private var angelClient: com.tencent.angel.client.AngelPSClient = _
  private var hookTask: Runnable = _

  def apply(executorId: String, sparkConf: SparkConf): AngelPSClient = {
    if (executorId == "driver") {
      new AngelPSClient(-1, submit(sparkConf))
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
      new AngelPSClient(executorId.toInt - 1, angelContext)
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

    val psJars = conf.get("spark.ps.jars")
    val psTmp = conf.get("spark.ps.tmp.path")
    val psOut = conf.get("spark.ps.out.path")
    val modelPath = conf.get("spark.ps.model.path")
    val psOutOverwrite = conf.getBoolean("spark.ps.out.overwrite", true)
    val psOutTmpOption = conf.getOption("spark.ps.out.tmp.path")

    val psLogLevel = conf.get("spark.ps.log.level", "INFO")

    val psClass = conf.get("spark.ps.class",
      "com.tencent.angel.ps.impl.ParameterServer")
    val psPartitionerClass = conf.get("spark.ps.partitioner.class",
      "com.tencent.angel.ps.HashPSPartitioner")


    import com.tencent.angel.conf.AngelConfiguration._

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    hadoopConf.set(ANGEL_RUNNING_MODE, "ANGEL_PS")

    hadoopConf.set(ANGEL_JOB_NAME, appName)
    hadoopConf.set(ANGEL_QUEUE, queue)

    hadoopConf.set(ANGEL_DEPLOY_MODE, deployMode)
    hadoopConf.setInt(ANGEL_PS_NUMBER, psNum)
    hadoopConf.setInt(ANGEL_PS_CPU_VCORES, psCores)
    hadoopConf.setInt(ANGEL_PS_MERMORY_MB, psMem)
    hadoopConf.set(ANGEL_PS_JAVA_OPTS, psOpts)
    hadoopConf.setInt(TOTAL_CORES, psNum * psCores)

    hadoopConf.set(ANGEL_JOB_LIBJARS, psJars)
    hadoopConf.set(ANGEL_STAGING_DIR, psTmp)
    hadoopConf.set(ANGEL_JOB_OUTPUT_PATH, psOut)
    hadoopConf.setBoolean(ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, psOutOverwrite)
    psOutTmpOption.foreach {
      hadoopConf.set(ANGEL_JOB_TMP_OUTPUT_DIRECTORY, _)
    }

    hadoopConf.set(ANGEL_AM_LOG_LEVEL, psLogLevel)
    hadoopConf.set(ANGEL_PS_LOG_LEVEL, psLogLevel)

    hadoopConf.set(ANGEL_PS_CLASS, psClass)

    hadoopConf
  }

  private def submit(conf: SparkConf): AngelContext = {
    angelClient = new com.tencent.angel.client.AngelPSClient(convertToHadoop(conf))
    hookTask = new Runnable {
      def run(): Unit = doStop()
    }
    ShutdownHookManager.get().addShutdownHook(hookTask,
      FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)
    angelClient.startPS(true)
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
    if (angelClient != null) true else false
  }

  private def doStop(): Unit = {
    if (angelClient != null) {
      angelClient.stopPS()
      angelClient = null
    }
  }

}
