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
package com.tencent.angel.spark.ml.embedding

import com.tencent.angel.graph.client.node2vec.PartitionHasher
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.node2vec.{Node2Vec, Node2VecModel}
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class Node2VecSuite extends PSFunSuite with SharedPSContext {
  val input = "../../data/bc/edge"

  val customSchema = StructType(Array(
    StructField("src", LongType, nullable = false),
    StructField("dst", LongType, nullable = false))
  )

  var psMatrix: PSMatrix = _

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  def stopPS(): Unit = {
    PSContext.stop()
  }

  test("read data") {
    val data = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(customSchema)
      .load(input)
    data.printSchema()
    data.show(50)
    data.agg(max("src"), max("dst")).show()
    // 10311
  }

  test("Node2Vec") {
    val start = System.currentTimeMillis()
    val data = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(customSchema)
      .load(input)

    val n2v = new Node2Vec()
      .setPSPartitionNum(2)
      .setPartitionNum(4)
      .setHitRatio(0.2)
      .setWalkLength(30)
      .setNeedReplicaEdge(true)

    val model = n2v.fit(data)

    val startSave = System.currentTimeMillis()
    model.write.overwrite().save("../../n2v")
    val endSave = System.currentTimeMillis()
    println(s"model save time: ${(endSave - startSave) / 1000.0}")


    println(s"sample finished!")

    val end = System.currentTimeMillis()
    println(s"the elapsed time: ${1.0 * (end - start) / 1000}")
  }

  test("save untrained model") {
    val n2v = new Node2Vec()
      .setPSPartitionNum(2)
      .setPartitionNum(4)
      .setNeedReplicaEdge(true)

    n2v.write.save("../../n2v2")
  }

  test("load and trained model") {
    val data = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", " ")
      .schema(customSchema)
      .load(input)

    val n2v = Node2Vec.load("../../n2v2")

    n2v.fit(data)
  }

  test("load path") {
    Node2VecModel.read.load("../../n2v")
    println(s"load finished!")
  }

  test("other") {
    println((0 to 10).mkString("{", ", ", "}"))
    println((0 until 10).mkString("{", ", ", "}"))
    val pairs = Array((1949L, 8910L), (738L, 795L), (1547L, 1931L), (85L, 7582L), (715L, 9503L), (175L, 1049L),
      (673L, 5240L), (445L, 602L), (2098L, 8867L), (1804L, 3478L), (1780L, 6174L), (1057L, 6072L), (2167L, 3525L),
      (685L, 9918L), (2243L, 10031L))
    pairs.foreach { case (e1, e2) =>
      println(PartitionHasher.getHash(e2, e1, 4))
    }

  }
}
