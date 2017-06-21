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

package com.tencent.angel.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import com.tencent.angel.spark.PSContext

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

  override def beforeAll() {
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

    // Spark setup
    val builder = SparkSession.builder()
      .master("local[2]")
      .appName("test")
      .config(psConf)

    _spark = builder.getOrCreate()
    sc.setLogLevel("OFF")

    // PS setup
    PSContext.getOrCreate(sc)
  }

  override def afterAll() {
    try {
      PSContext.stop()
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