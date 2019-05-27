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

package com.tencent.angel.spark.automl

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.operator.{MetadataTransformUtils, VectorCartesian}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class MetadataTest {

  val spark = SparkSession.builder().master("local").getOrCreate()

  @Test
  def testVectorCartesian(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", "123")
      .load("../../data/a9a/a9a_123d_train_trans.libsvm")
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("cartesian_features")

    val assembler = new VectorAssembler()
      .setInputCols(Array("features", "cartesian_features"))
      .setOutputCol("assemble_features")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, assembler))

    val featureModel = pipeline.fit(data)
    val crossDF = featureModel.transform(data)

    crossDF.schema.fields.foreach{ field =>
      println("name: " + field.name)
      println("metadata: " + field.metadata.toString())
    }

    spark.close()

  }

  @Test
  def testThreeOrderCartesian(): Unit = {
    val data = spark.read.format("libsvm")
      .option("numFeatures", 8)
      .load("../../data/abalone/abalone_8d_train.libsvm")
      .persist()

    val cartesian = new VectorCartesian()
      .setInputCols(Array("features", "features"))
      .setOutputCol("f_f")

    val cartesian2 = new VectorCartesian()
      .setInputCols(Array("features", "f_f"))
      .setOutputCol("f_f_f")

    val pipeline = new Pipeline()
      .setStages(Array(cartesian, cartesian2))

    val crossDF = pipeline.fit(data).transform(data).persist()

    // first cartesian, the number of dimensions is 64
    println(crossDF.select("f_f").schema.fields.last.metadata.getStringArray(MetadataTransformUtils.DERIVATION).length)
    // second cartesian, the number of dimensions is 512
    println(crossDF.select("f_f_f").schema.fields.last.metadata.getStringArray(MetadataTransformUtils.DERIVATION).length)


    crossDF.select("f_f").schema.fields.last.metadata.getStringArray(MetadataTransformUtils.DERIVATION).foreach(str => println(str + ", "))
    println()
    crossDF.select("f_f_f").schema.fields.last.metadata.getStringArray(MetadataTransformUtils.DERIVATION).foreach(str => println(str + ", "))

    spark.close()
  }

}
