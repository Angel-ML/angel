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

package com.tencent.angel.spark.examples.basic

import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{Row, SparkSession}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.ml.math2.vector.IntDoubleVector
import com.tencent.angel.spark.util.VectorUtils

object LR {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val ITERATIONS = params.getOrElse("numIter", "5").toInt
    val input = params.getOrElse("input", "data/a9a/a9a_123d_train.libsvm")
    val mode = params.getOrElse("mode", "yarn-cluster")
    val lr = params.getOrElse("lr", "0.1").toDouble

    val ss = SparkSession.builder()
      .master(mode)
      .appName("LRExample").getOrCreate()
    val sc = ss.sparkContext
    val psc = PSContext.getOrCreate(sc)

    val data = ss.read.format("libsvm").load(input)
    val firstRow = data.select("features").take(1)
    val numFeatures = firstRow(0).getAs[MLVector](0).size
    val trainData = data.rdd.map { case Row(label: Double, v: MLVector) =>
      (VFactory.sparseDoubleVector(numFeatures, v.toSparse.indices, v.toSparse.values), label)
    }.cache()
    val psW = PSVector.dense(numFeatures) // weights
    val psG = PSVector.duplicate(psW) // gradients of weights

    println("Initial psW: " + psW.dimension)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val localW = psW.pull()
      trainData.map { case (x, label) =>
        val g = x.mul(-label * (1 - 1.0 / (1.0 + math.exp(-label * localW.dot(x)))))
        psG.increment(g)
      }.count()
      VectorUtils.axpy(-lr / numFeatures, psG, psW)
      psG.reset
    }
    println(s"Final psW: ${psW.pull().asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" ")}")
    PSContext.stop()
    sc.stop()
  }
}
