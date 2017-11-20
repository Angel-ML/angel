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

    val dataset = DataLoader.loadLibsvm(input, 2, 1.0, -1)
      .map { case (label, feat) => Instance(label, feat, 1.0) }

    val featureNum = dataset.first().feature.size
    param = new GBTreeParam()
    param.partitionNum = 2
    param.featureNum = featureNum
    param.splitNum = 5
    param.featureSampleRate = 1.0
    param.maxDepth = 4
    param.maxTreeNum = 10
    param.minChildWeight = 2
    param.learningRate = 0.3

    println(s"feature num: ${param.featureNum}")
    learner = new GBDTLearner(param)
    instances = learner.init(dataset)._1
    tree = learner.createNewTree(0, instances)

  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("create sketch") {
    sketches = learner.createSketch(instances)

    println(s"sketch: \n${sketches.map(_.mkString(" ")).mkString("\n")}")

    instances.foreach { case (id, instance) =>
      println(s"$id  instance: ${instance.toString}")
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

    learner.param.loss = new LeastSquareLoss

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

    tree.forActive { node =>
      val histogram = learner.gradHistMatrix.pull(node.id)
      println(s"histogram node ${node.id}: ${histogram.mkString(" ")}")
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
    println(s"instance layout:")
    val tempRDD = instances.mapPartitionsWithIndex { case (pId, iter) =>
      println(s"partition $pId instance:")
      println(s"${iter.map(x => "index:" + x._1 + " " + x._2.toString).mkString("\n")}")
      Iterator.empty
    }
    tempRDD.count()

    println(s"instance layout:")
    instances.partitions.indices.foreach { pId =>
      println(s"partition $pId position info: ${learner.instancePosInfoMat.pull(pId).mkString(" ")}")
      println(s"partition $pId layout: ${learner.instanceLayoutMat.pull(pId).mkString(" ")}")
    }

    learner.growTree(tree, instances)

    println(s"instance layout:")
    instances.partitions.indices.foreach { pId =>
      println(s"partition $pId position info: ${learner.instancePosInfoMat.pull(pId).mkString(" ")}")
      println(s"partition $pId layout: ${learner.instanceLayoutMat.pull(pId).mkString(" ")}")
    }

    println(s"tree after grow: ${tree.toString}")
  }

  test("train") {
    val gbtModel = learner.train(instances.map(_._2))
    println(s"gbt model ${gbtModel.toString}")
  }

}
