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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.PSFunSuite

class PSContextSuite extends PSFunSuite {
  // PS constant parameter
  val dim = 10
  val capacity = 10
  var _spark: SparkSession = _

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

    // Spark setup
    val builder = SparkSession.builder()
      .master("local[2]")
      .appName("test")
      .config(psConf)

    _spark = builder.getOrCreate()
    _spark.sparkContext.setLogLevel("OFF")
  }


  test("getOrCreate/stop") {
    val psContext = PSContext.getOrCreate(_spark.sparkContext)

    assert(psContext != null)

    PSContext.stop()
  }

  test("create vector pool") {
    val psContext = PSContext.getOrCreate()
    val pool = psContext.createModelPool(dim, capacity)

    val proxy = pool.createZero()

    assert(pool.numDimensions == dim)
    assert(pool.capacity == capacity)
    assert(proxy.mkRemote().size == dim)
    assert(proxy.mkRemote().pull().sameElements(Array.ofDim[Double](dim)))

    psContext.destroyModelPool(pool)
  }

  test("destroy vector pool") {
    val psContext = PSContext.getOrCreate()
    val pool = psContext.createModelPool(dim, capacity)

    psContext.destroyModelPool(pool)
    assert(psContext.getPool(pool.id) == null)
  }

}
