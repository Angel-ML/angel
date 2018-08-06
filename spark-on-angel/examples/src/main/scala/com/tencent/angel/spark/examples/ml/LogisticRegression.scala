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

package com.tencent.angel.spark.examples.ml

import org.apache.spark.ml.classification.ps.{LogisticRegression => PSLR}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.examples.util.SparkUtils._

object LogisticRegression {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)

      run(N, DIM, numSlices, ITERATIONS)
    }
  }

  case class Instance(label: Double, weight: Double, feature: Vector)

  def run(sampleNum: Int, featNum: Int, partitionNum: Int, maxIter: Int, regParam: Double = 0.01) {
    val tol = 1e-6
    val elasticNet = 0.0

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val trainSet = Logistic.generateLRData(sampleNum, featNum, partitionNum)
      .map { case (feat, label) => Instance(label, 1.0, feat)}
      .toDS()

    val lor = new PSLR()
      .setFeaturesCol("feature")
      .setWeightCol("weight")
      .setLabelCol("label")
      .setRegParam(regParam)
      .setElasticNetParam(elasticNet)
      .setMaxIter(maxIter)
      .setTol(tol)

    val lorModel = lor.fit(trainSet)
    println(s"model size: ${lorModel.coefficientMatrix.toArray.length}")
    println(s"trained model: ${lorModel.coefficientMatrix.toArray.mkString(" ")}")
    println(s"intercept: ${lorModel.interceptVector.toArray.mkString(" ")}")
  }
}

// scalastyle:on println
