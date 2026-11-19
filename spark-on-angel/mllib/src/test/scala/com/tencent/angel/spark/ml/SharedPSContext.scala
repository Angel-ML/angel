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


package com.tencent.angel.spark.ml

import java.nio.file.{Files, Path, Paths}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import com.tencent.angel.spark.context.PSContext

/**
  * Shares a local `SparkSession and PSClient`
  * between all tests in a suite and closes it at the end
  */
trait SharedPSContext extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  @transient private var _spark: SparkSession = _

  def doubleEps: Double = 1e-6

  def spark: SparkSession = _spark

  def sc: SparkContext = _spark.sparkContext

  var conf = new SparkConf(false)

  @transient protected lazy val testDirectory: Path =
    Files.createTempDirectory(getClass.getSimpleName + "-")

  protected def testOutputPath(name: String): String =
    testDirectory.resolve(name).toUri.toString

  protected def testDataPath(relative: String): String = {
    var root = Paths.get(System.getProperty("user.dir")).toAbsolutePath
    while (root != null && !(Files.isDirectory(root.resolve("spark-on-angel")) &&
      Files.isDirectory(root.resolve("angel-ps")))) {
      root = root.getParent
    }
    require(root != null, "Cannot locate the Angel repository root")
    val path = root.resolve("data").resolve(relative)
    require(Files.isRegularFile(path), s"Missing test data: $path")
    path.toUri.toString
  }

  override def beforeAll() {
    super.beforeAll()

    // Angel config
    val psConf = new SparkConf()
      .set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.tmp.path", testOutputPath("stage"))
      .set("spark.ps.out.path", testOutputPath("output"))
      .set("spark.ps.model.path", testOutputPath("model"))
      .set("spark.hadoop.fs.defaultFS", "file:///")
      .set("spark.ps.instances", "2")
      .set("spark.ps.cores", "1")
      .set("spark.ps.out.tmp.path.prefix", testDirectory.resolve("ps-tmp").toString)
      .set("angel.userrequest.log.mode", "detail")
      .set("angel.rpc.log.mode", "detail")

    // Spark setup
    val builder = SparkSession.builder()
      .master("local[4]")
      .appName("test")
      .config(psConf)
      .config(conf)

    _spark = builder.getOrCreate()
    sc.setLogLevel("ERROR")

    // PS setup
    PSContext.getOrCreate(sc)
  }

  override def afterAll() {
    try {
      try PSContext.stop()
      finally {
        if (_spark != null) _spark.stop()
        _spark = null
      }
    } finally {
      try FileUtils.deleteDirectory(testDirectory.toFile)
      finally super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
  }
}
