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

package com.tencent.angel.spark.ml.gbt

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.ml.common.Instance
import com.tencent.angel.spark.ml.util.DataLoader
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}


class GBDTLearnerSuite extends PSFunSuite with SharedPSContext with Serializable {

  private val input = "./src/test/data/agaricus.txt.train"
  private var instances: RDD[(Long, Instance)] = _
  private var param: GBTreeParam = _
  private var learner: GBDTLearner = _

  // env
  private var tree: Tree = _
  private var sketches: Array[Array[Double]] = _
  private var gradHessRDD: RDD[(Long, (Double, Double))] = _
  private var sampleFeature: Broadcast[Array[Int]] = _
  private var sampleSketch: Broadcast[Array[Array[Double]]] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val dataSet = DataLoader.loadLibsvm(input, 2, 1.0, -1)
      .sample(false, 0.1)
      .map { case (label, feat) => Instance(label, feat, 1.0) }

    val featureNum = dataSet.first().feature.size
    param = new GBTreeParam()
    param.partitionNum = 2
    param.featureNum = featureNum
    param.splitNum = 2
    param.featureSampleRate = 1.0
    param.maxDepth = 5
    param.maxTreeNum = 2
    param.minChildWeight = 0.1
    param.learningRate = 0.3
    param.validateFraction = 0.0
    param.loss = new LeastSquareLoss

    println(s"data set size: ${dataSet.count()}")
    println(s"feature num: ${param.featureNum}")
    learner = new GBDTLearner(param)
    instances = learner.init(dataSet)._1
    tree = learner.createNewTree(0, instances)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("create sketch") {
    sketches = learner.createSketch(instances)

    // fake sketch
    sketches = Range(0, param.featureNum).toArray.map { i =>
      Array(-0.5, 0.5)
    }
    println(s"sketch: \n${sketches.slice(0, 10).map(_.mkString(" ")).mkString("\n")}")

    instances.take(10).foreach { case (id, instance) =>
      println(s"$id instance: ${instance.toString}")
    }
  }

  test("sample feature") {
    val temp = learner.sampleFeature(sketches)
    sampleFeature = temp._1
    sampleSketch = temp._2
    val localFeat = sampleFeature.value

    println(s"sampled feature: ${localFeat.mkString(" ")}")
    assert(localFeat.length == learner.param.sampledFeatNum)
    assert(localFeat.sorted.sameElements(localFeat))
  }

  test("calculate gradient") {
    val predictions = instances.map { case (id, instance) => Tuple2(id, 0.0) }

    this.gradHessRDD = learner.calculateGradient(instances, predictions)


    this.gradHessRDD.join(instances).collect()
      .foreach { case (id, ((grad, hess), instance)) =>
        assert(grad == 0.0 - instance.label)
        assert(hess == 1.0)
      }
  }

  test("build histogram") {
    learner.buildHistogram(tree, instances, gradHessRDD, sampleFeature, sampleSketch)

    val positiveSampleNum = instances.map { case (index, instance) => instance.label }.sum()
    val instanceNum = instances.count()
    tree.forActive { node =>
      val histogram = learner.gradHistMatrix.pull(node.id)
      sampleFeature.value.indices.foreach { i =>
        assert(histogram(i * 4 + 0) + histogram(i * 4 + 1) == -1 * positiveSampleNum)
        assert(histogram(i * 4 + 2) + histogram(i * 4 + 3) == instanceNum)
      }
    }
  }

  test("find split") {
    learner.findSplit(tree, sampleFeature, sampleSketch)

    val splitFeature = learner.gbtModel.getSplitFeature(tree.id)
    val splitValue = learner.gbtModel.getSplitValue(tree.id)
    val splitGain = learner.splitGainMat.pull(tree.id)
    val gradHess = learner.gradHessMat.pull(tree.id)

    println(s"split feature: ${splitFeature.mkString(" ")}")
    println(s"split value: ${splitValue.mkString(" ")}")
    println(s"split gain: ${splitGain.mkString(" ")}")
    println(s"grad hess: ${gradHess.mkString(" ")}")
  }

  test("grow tree") {
    learner.growTree(tree, instances)

    println(s"tree after grow: ${tree.toString}")
  }

  test("train") {
    param.loss = new LogisticLoss
    val gbtModel = learner.train(instances.map(_._2))
    println(s"gbt model ${gbtModel.toString}")
  }

}
