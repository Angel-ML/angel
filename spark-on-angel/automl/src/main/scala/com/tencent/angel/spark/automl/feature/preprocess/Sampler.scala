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


package com.tencent.angel.spark.automl.feature.preprocess

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.util.Random


class Sampler(fraction: Double,
              override val uid: String,
              seed: Int = Random.nextInt)
  extends Transformer {

  def this(fraction: Double) = this(fraction, Identifiable.randomUID("sampler"))

  /**
    * Param for input column name.
    *
    * @group param
    */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group setParam */
  final def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group getParam */
  final def getInputCol: String = $(inputCol)

  /** @group getParam */
  final def getOutputCol: String = $(inputCol)

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.sample(false, fraction, seed).toDF
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Sampler = defaultCopy(extra)
}

object Sampler {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder
      .master("local")
      .appName("preprocess")
      .getOrCreate()

    val training = ss.read.format("libsvm")
      .load("/Users/jiangjiawei/dev-tools/spark-2.2.0/data/mllib/sample_libsvm_data.txt")

    println(training.count)

    val sampler = new Sampler(0.5)
      .setInputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(sampler))

    val model = pipeline.fit(training)

    val test = ss.read.format("libsvm")
      .load("/Users/jiangjiawei/dev-tools/spark-2.2.0/data/mllib/sample_libsvm_data.txt")

    model.transform(test).select("*")
      .collect()
      .foreach { case Row(label: Double, vector: Vector) =>
        println(s"($label, " +
          s"${vector.toSparse.indices.mkString("[", ",", "]")}, " +
          s"${vector.toSparse.values.mkString("[", ",", "]")}")
      }

    ss.stop()
  }
}