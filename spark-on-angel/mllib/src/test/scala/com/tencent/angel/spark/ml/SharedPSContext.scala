package com.tencent.angel.spark.ml

import com.tencent.angel.spark.PSClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Shares a local `SparkSession and PSClient` between all tests in a suite and closes it at the end */
trait SharedPSContext extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  @transient private var _spark: SparkSession = _

  def doubleEps: Double = 1e-6

  def spark: SparkSession = _spark

  def sc: SparkContext = _spark.sparkContext

  var conf = new SparkConf(false)

  override def beforeAll() {
    super.beforeAll()

    // Spark setup
    val builder = SparkSession.builder()
      .master("local[2]")
      .appName("test")

    _spark = builder.getOrCreate()
    _spark.sparkContext.setLogLevel("OFF")

    // PS setup
    PSClient.setup(_spark.sparkContext)
  }

  override def afterAll() {
    try {
      PSClient.cleanup(_spark.sparkContext)
      _spark.stop()
      _spark = null
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
  }
}