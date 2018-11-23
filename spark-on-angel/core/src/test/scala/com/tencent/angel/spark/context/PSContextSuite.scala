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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.PSFunSuite

class PSContextSuite extends PSFunSuite {

  private val dim = 10
  private val capacity = 10
  private val rows = 20
  private val cols = 20
  private val rowInBlock = 10
  private val colInBlock = 10
  private var angel: PSContext = _
  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Angel config
    val psConf = new SparkConf()
      .set("spark.ps.mode", "LOCAL")
      .set("spark.ps.jars", "None")
      .set("spark.ps.instances", "2")
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
    sparkSession = spark
    angel = PSContext.getOrCreate(spark.sparkContext).asInstanceOf[AngelPSContext]
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    PSContext.stop()
    super.afterAll()
  }

  test("start & stop Angel") {

    // alive
    assert(AngelPSContext.isAlive)

    // stop angel
    PSContext.stop()
    assert(!AngelPSContext.isAlive)

    // restart angel
    angel = PSContext.getOrCreate(sparkSession.sparkContext)
    assert(AngelPSContext.isAlive)
  }

  test("create matrix and destroy matrix") {
    val meta = angel.createMatrix(rows, cols, -1, rowInBlock, colInBlock, RowType.T_FLOAT_DENSE, Map())
    assert(meta.getColNum == cols)
    assert(meta.getRowNum == rows)
    assert(meta.getRowType == RowType.T_FLOAT_DENSE)
    angel.destroyMatrix(meta.getId)
  }

}
