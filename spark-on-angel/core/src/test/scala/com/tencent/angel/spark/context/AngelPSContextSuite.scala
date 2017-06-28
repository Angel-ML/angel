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

import com.tencent.angel.spark.{PSContext, PSFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

class AngelPSContextSuite extends PSFunSuite {

  private val dim = 10
  private val capacity = 10

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Angel config
    val psConf = new SparkConf()
      .set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.instances", "1")
      .set("spark.ps.cores", "1")
      .set("spark.ps.log.level", "INFO")

    // Spark config
    val builder = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .config(psConf)

    // start Spark
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
  }

  override def afterAll(): Unit = {
    SparkSession.builder().getOrCreate().stop()

    super.afterAll()
  }

  test("start & stop Angel") {
    // start Angel
    val spark = SparkSession.builder().getOrCreate()
    PSContext.getOrCreate(spark.sparkContext)
    val angel = PSContext.getOrCreate().asInstanceOf[AngelPSContext]

    assert(AngelPSContext.isAlive)

    // create pool
    val pool = angel.createModelPool(dim, capacity)
    angel.destroyModelPool(pool)

    PSContext.stop()
    assert(!AngelPSContext.isAlive)
  }

  test("doCreateVectorPool && doDestroyVectorPool") {
    val spark = SparkSession.builder().getOrCreate()
    PSContext.getOrCreate(spark.sparkContext)
    val angel = PSContext.getOrCreate().asInstanceOf[AngelPSContext]

    val thisPool = angel.createModelPool(dim, capacity)
    val zeroVector = thisPool.createZero()

    assert(thisPool.numDimensions == dim)
    assert(thisPool.capacity == capacity)
    assert(zeroVector.getPool().id == thisPool.id)
    assert(zeroVector.mkRemote().pull().sameElements(Array.ofDim[Double](dim)))

    angel.destroyModelPool(thisPool)
    assert(angel.getPool(thisPool.id) == null)

    PSContext.stop()
  }

}
