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

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.util.DataLoader
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.ml.math2.vector.Vector

object LogisticRegression {

  def start(mode: Option[String] = None): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode.getOrElse(conf.get("spark.master", "local[1]")))
    conf.setAppName("PS LogisticRegression")
    conf.setIfMissing("spark.ps.instances", "1")
    conf.setIfMissing("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def calculate(weight: Vector, data: Array[LabeledData]): Vector = {
    val gradient = com.tencent.angel.ml.math2.VFactory.sparseFloatVector(weight.dim().toInt)
    data.foreach { sample =>
      val margin = sample.getY * weight.dot(sample.getX)
      val scale = if (margin >= 0) {
        val e = math.exp(-margin)
        -sample.getY * e / (1.0 + e)
      } else -sample.getY / (1.0 + math.exp(margin))
      gradient.iaxpy(sample.getX, scale)
    }
    gradient
  }


  def parseSample(line: String, dim: Int, indexBase: Int): LabeledData = {
    val tokens = line.trim.split("\\s+")
    val features = com.tencent.angel.ml.math2.VFactory.sparseFloatVector(dim)
    tokens.tail.foreach { token =>
      val pair = token.split(":", 2)
      require(pair.length == 2, s"Invalid feature: $token")
      val index = pair(0).toInt - indexBase
      require(index >= 0 && index < dim, s"Feature index $index is outside [0,$dim)")
      features.set(index, pair(1).toFloat)
    }
    val label = tokens(0).toDouble
    require(label == 0.0 || label == 1.0 || label == -1.0, "Binary labels must be 0/1 or -1/+1")
    new LabeledData(features, if (label == 0.0) -1.0 else label)
  }

  def main(args: Array[String]): Unit = {
    val params = com.tencent.angel.spark.ml.core.ArgsUtil.parse(args)
    val input = params.getOrElse("input", throw new IllegalArgumentException("input is required"))
    val dim = params.getOrElse("dim", "10").toInt
    val indexBase = params.getOrElse("indexBase", "1").toInt
    require(indexBase == 0 || indexBase == 1, "indexBase must be 0 or 1")
    val iterations = params.getOrElse("numIter", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    require(dim > 0 && iterations > 0 && sampleRate > 0 && sampleRate <= 1, "Invalid training parameters")
    start(params.get("mode"))
    try {
    val sc = SparkContext.getOrCreate()
    val data = sc.textFile(input).filter(_.trim.nonEmpty).map(f => parseSample(f, dim, indexBase)).cache()
    require(data.count() > 0, "Training input is empty")
    val mat = PSMatrix.rand(4, dim, RowType.T_FLOAT_DENSE)
    mat.reset(Array(0, 1, 2, 3))
    val lr = params.get("stepSize").map(_.toDouble)
      .getOrElse(sc.getConf.getDouble("spark.hadoop.ml.learn.rate", 0.01))
    require(lr > 0 && !lr.isNaN && !lr.isInfinity, "stepSize must be finite and positive")
    val gamma: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_GAMMA)
    val beta: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_BETA)
    val opt = new Adam(lr, gamma, beta)

    for (iteration <- 0 until iterations) {
      mat.reset(3)
      val size = data.sample(false, sampleRate, 42L + iteration).mapPartitions { iter =>
        PSContext.instance()
        var count = 0
        iter.grouped(1024).foreach { samples =>
          val weight = mat.pull(0)
          val gradient = calculate(weight, samples.toArray)
          mat.increment(3, gradient)
          count += samples.length
        }
        Iterator.single(count)
      }.fold(0)(_ + _)
      if (size > 0) opt.update(mat.id, 1, iteration + 1, size).get()
    }
    val weights = mat.pull(0)
    val values = weights.asInstanceOf[com.tencent.angel.ml.math2.vector.IntFloatVector].getStorage.getValues
    require(values.forall(x => !x.isNaN && !x.isInfinity), "Training produced non-finite weights")
    params.get("output").foreach { output =>
      sc.parallelize(0 until dim, 1).map(i => s"$i,${weights.asInstanceOf[com.tencent.angel.ml.math2.vector.IntFloatVector].get(i)}")
        .saveAsTextFile(output)
    }
    println(s"BASIC_LR_TRAIN_PASS iterations=$iterations weightNorm=${math.sqrt(values.map(x => x.toDouble * x).sum)}")
    data.unpersist()
    } finally { stop() }
  }

}
