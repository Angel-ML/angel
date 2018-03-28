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

import com.tencent.angel.spark.PSFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.vector.VectorType

class AngelPSContextSuite extends PSFunSuite {

  private val dim = 10
  private val capacity = 10
  private val rows = 10
  private val cols = 10
  private var angel: AngelPSContext = _

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
    angel = PSContext.getOrCreate(spark.sparkContext).asInstanceOf[AngelPSContext]
  }

  override def afterAll(): Unit = {
    SparkSession.builder().getOrCreate().stop()

    super.afterAll()
  }

  test("start & stop Angel") {
    assert(AngelPSContext.isAlive)
    PSContext.stop()
    assert(!AngelPSContext.isAlive)

    val spark = SparkSession.builder().getOrCreate()
    angel = PSContext.getOrCreate(spark.sparkContext).asInstanceOf[AngelPSContext]
    assert(AngelPSContext.isAlive)
  }

  test("create matrix and destroy matrix") {
    val meta = angel.createDenseMatrix(rows, cols)

    assert(meta.getColNum == cols)
    assert(meta.getRowNum == rows)
    assert(meta.getRowType == RowType.T_DOUBLE_DENSE)

    angel.destroyMatrix(meta.getId)
  }

  test("doCreateVectorPool && doDestroyVectorPool") {

    val thisPool = angel.createVectorPool(dim, capacity, VectorType.DENSE, -1)
    val firstVector = thisPool.allocate()

    assert(thisPool.vType == VectorType.DENSE)
    assert(thisPool.size == 1)
    assert(thisPool.id == firstVector.poolId)
    assert(thisPool.dimension == firstVector.dimension)
    assert(firstVector.id == 0)
    assert(!thisPool.destroyed)

    angel.destroyVectorPool(thisPool.id)

    assert(thisPool.destroyed)
    assert(angel.getPool(thisPool.id) == null)
  }

}
