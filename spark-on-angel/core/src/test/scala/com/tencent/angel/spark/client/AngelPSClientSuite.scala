package com.tencent.angel.spark.client

import com.tencent.angel.spark.{PSClient, PSFunSuite, PSVectorPool}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkEnv}

class AngelPSClientSuite extends PSFunSuite {

  private val dim = 10
  private val capacity = 10
  private var _angel: AngelPSClient = _
  private var _pool: PSVectorPool = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Angel config
    val psConf = new SparkConf()
      .set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.tmp.path", "file:///tmp/stage")
      .set("spark.ps.out.path", "file:///tmp/output")
      .set("spark.ps.model.path", "file:///tmp/model")
      .set("spark.ps.instances", "1")
      .set("spark.ps.cores", "1")

    // Spark config
    val builder = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .config(psConf)

    // start Spark
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    // start Angel
    PSClient.setup(spark.sparkContext)
    _angel = PSClient.get.asInstanceOf[AngelPSClient]

    // create pool
    _pool = _angel.createVectorPool(dim, capacity)
  }

  override def afterAll(): Unit = {
    _angel.destroyVectorPool(_pool)
    _pool = null
    AngelPSClient.stop()
    _angel = null
    SparkSession.builder().getOrCreate().stop()
    super.afterAll()
  }

  test("start & stop Angel") {
    _angel.destroyVectorPool(_pool)
    AngelPSClient.stop()
    assert(!AngelPSClient.isAlive)

    val env = SparkEnv.get
    _angel = AngelPSClient(env.executorId, env.conf)
    _pool = _angel.createVectorPool(dim, capacity)
    assert(AngelPSClient.isAlive)
  }

  test("doCreateVectorPool && doDestroyVectorPool") {
    val thisPool = _angel.createVectorPool(dim, capacity)
    val zeroVector = thisPool.createZero()

    assert(thisPool.numDimensions == dim)
    assert(thisPool.capacity == capacity)
    assert(zeroVector.getPool().id == thisPool.id)
    assert(zeroVector.mkLocal().get().sameElements(Array.ofDim[Double](dim)))

    _angel.destroyVectorPool(thisPool)
    assert(_angel.getPool(thisPool.id) == null)
  }
}
