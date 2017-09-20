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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.yahoo.sketches.quantiles.DoublesSketch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.ml.common.Instance
import com.tencent.angel.spark.math.matrix.{DensePSMatrix, PSMatrix}

class GBDTController {

  // constant parameter
  private var featNum: Int = _
  private var splitNum: Int = _
  private var maxTreeNum: Int = _
  private var maxNodeNum: Int = _
  private var featSampleRate: Double = _

  // environment parameter
  private var currentTree: Int = 0

  private var sketchMatrix: DensePSMatrix = _
  private val sampleFeatNum = (featSampleRate * featNum).toInt
  private val sampledFeatMatrix = PSMatrix.dense(maxTreeNum, sampleFeatNum)

  private var sampledFeatBC: Broadcast[Array[Int]] = _

  private var node2Instance: DensePSMatrix = _
  private var partInstancePos: DensePSMatrix = _
  private var tree: Tree = _

  def init(trainSet: RDD[Instance], partitionNum: Int): RDD[(Long, Instance)] = {
    val repRDD = if (trainSet.partitions.length != partitionNum) {
      trainSet.repartition(partitionNum)
    } else {
      trainSet
    }

    val instances = repRDD.zipWithIndex()
      .map(x => (x._2, x._1))
      .cache()

    instances.count()

    val maxInstNum = instances.mapPartitions(iter => Iterator.single(iter.length)).max()

    node2Instance = PSMatrix.dense(partitionNum, maxInstNum)
    partInstancePos = PSMatrix.dense(partitionNum, maxNodeNum * 2)

    sketchMatrix = PSMatrix.dense(partitionNum, featNum * splitNum)

    instances
  }



  /**
   * Create data sketch, push candidate split value to PS
   */
  def createSketch(instances: RDD[(Long, Instance)]): Broadcast[Array[Array[Double]]] = {
    val sketch = splitWithYahooSketch(instances, splitNum)
    val sketchBC = instances.context.broadcast(sketch)
    sketchBC
  }

  /**
   * Sample feature
   * different tree will base on different feature.
   */
  def sampleFeature(sampleFeatNum: Int): Broadcast[Array[Int]] = {
    val feats = if (featSampleRate < 1.0) {
      val sampledFeat = new ArrayBuffer[Int]
      val rand = new Random()
      (0 until featNum).foreach { id =>
        if (rand.nextDouble() < featSampleRate) {
          sampledFeat.append(id)
        }
      }
      sampledFeat.toArray
    } else {
      (0 until featNum).toArray
    }
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.broadcast(feats)
  }

  /**
   * Create a tree
   */
  def createTree(): Unit = {
    // 1. init a RegressionTree,  new tree and add to forest
    tree = new Tree(maxNodeNum)
    // 2. getFeatSet
    val featSet = sampledFeatMatrix.pull(currentTree)

    // 3. Calculate gradient
    // for each partition, gradient for each instance will be calculate.
    val gradPair = calGradient()
  }

  /**
   * calculate Gradient and Hessian
   */
  def calculateGradient(
      instances: RDD[(Long, Instance)],
      prediction: RDD[(Long, Double)],
      loss: Loss): RDD[(Long, (Double, Double))] = {
    // TODO: instances and prediction have the same partitioner
    val instanceGradRDD = instances.join(prediction, instances.partitioner.get)
      .map { case (instId, (instance, pred)) =>
        val grad = loss.firstGradient(instance.label.toFloat, pred.toFloat)
        val hess = loss.secondGradient(instance.label.toFloat, pred.toFloat)
        (instId, (grad.toDouble, hess.toDouble))
      }

    instanceGradRDD.cache()
    instanceGradRDD.count()
    instanceGradRDD
  }

  def findFIdPosition(featSet: Array[Int], fId: Int): Int = {
    // featSet is sorted
    if (fId < featSet(0) || fId > featSet(featSet.length - 1)) {
      return -1
    }

    var begin = 0
    var end = featSet.length - 1

    while(end > begin + 1) {
      val mid = (end - begin) / 2
      if (featSet(mid) == fId) {
        return mid
      } else if (featSet(mid) > fId) {
        end = mid
      } else {
        begin = mid
      }
    }
    -1
  }

  def findFValuePosition(sketch: Array[Double], fValue: Double, begin: Int, end: Int): Int = {
    var offset = 1
    while(begin + offset <= end) {
      if (sketch(begin + offset) > fValue) {
        return offset - 1
      }
      offset += 1
    }
    end - begin
  }

  /**
   * Gradient and Hessian Histogram
   * There is a matrix for each node.
   * matrix(1, 2 * splitNum * sampleFeatNum)
   *
   * matrix num == node num
   * multi-thread to speed up
   */

  private val gradHistMatrix = DensePSMatrix(maxNodeNum, 2* splitNum * sampleFeatNum)

  def buildHistGraph(
      instances: RDD[(Long, Instance)],
      gradRDD: RDD[(Long, (Double, Double))]): Unit = {

    tree.forActive { node =>
      val tempRDD = instances.join(gradRDD, instances.partitioner.get)
        .mapPartitionsWithIndex { case (pId, iter) =>
          val instance2Grad = iter.toArray

          val nodePos = partInstancePos.pull(pId)
          val nodeStart = nodePos(node.id * 2).toInt
          val nodeEnd = nodePos(node.id * 2 + 1).toInt
          val instanceIds = node2Instance.pull(pId)

          val sketch = sketchMatrix.pull(pId)

          val featSet = sampledFeatBC.value
          val histogram = new Array[Double](featSet.length * 2 * splitNum)

          var gradSum = 0.0
          var hessSum = 0.0
          (nodeStart until nodeEnd).foreach { id =>
            val instance = instance2Grad(instanceIds(id).toInt)._2._1
            val (grad, hess) = instance2Grad(instanceIds(id).toInt)._2._2
            gradSum += grad
            hessSum += hess

            instance.feature.foreachActive { case (fId, fValue) =>
              val fPos = findFIdPosition(sampledFeatBC.value, fId)
              if (fPos != -1) {
                val fValueIndex =
                  findFValuePosition(sketch, fValue, fPos * splitNum, (fPos + 1) * splitNum - 1)
                val gradIndex = 2 * fPos * splitNum + fValueIndex
                val hessIndex = (2 * fPos + 1) * splitNum + fValueIndex
                histogram(gradIndex) += grad
                histogram(hessIndex) += hess

                val fZeroValueIndex =
                  findFValuePosition(sketch, 0.0, fPos * splitNum, (fPos + 1) * splitNum - 1)

                val zeroGradIndex = 2 * fPos * splitNum + fZeroValueIndex
                val zeroHessIndex = (2 * fPos + 1) * splitNum + fZeroValueIndex
                histogram(zeroGradIndex) -= grad
                histogram(zeroHessIndex) -= hess
              } else {
                // this feat has not been sampled.
              }
            }

            featSet.foreach { fPos =>
              val fZeroValueIndex =
                findFValuePosition(sketch, 0.0, fPos * splitNum, (fPos + 1) * splitNum - 1)
              val zeroGradIndex = 2 * fPos * splitNum + fZeroValueIndex
              val zeroHessIndex = (2 * fPos + 1) * splitNum + fZeroValueIndex
              histogram(zeroGradIndex) += gradSum
              histogram(zeroHessIndex) += hessSum
            }
          }

          gradHistMatrix.push(node.id, histogram)
          Iterator.empty
      }
      tempRDD.count()
    }
  }

  /**
   * implement executor splitter firstly
   * TODO: Server split
   */
  def findSplit(): Unit = {

    val activeNodes = tree.getActiveNode.map(node => node.id)

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.parallelize(activeNodes, activeNodes.length)
      .mapPartitions { nId =>


        Iterator.empty
      }

  }



  private def calGradient(): Array[GradPair] = ???



  case class GradPair(grad: Double, hess: Double)



  def splitWithYahooSketch(
      instances: RDD[(Long, Instance)],
      splitNum: Int): Array[Array[Double]] = {

    // sample some data to create sketch.
    val sketches = instances.mapPartitionsWithIndex { (pId, iter) =>
      val sketches = Array.fill(featNum)(DoublesSketch.builder().build())

      iter.foreach { case (id, point) =>
        point.feature.foreachActive { case (index, value) =>
          sketches(index).update(value)
        }
      }

      val fraction = (0 until splitNum).toArray.map(i => i.toDouble / splitNum)
      val quantiles = sketches.map { sk => sk.getQuantiles(fraction) }

      // quantiles is a two dimension matrix (featureNum * splitNum)
      quantiles.zipWithIndex.map(x => (x._2, x._1)).toIterator
    }.reduceByKey((skt1, skt2) => skt1.indices.toArray.map( i => skt1(i) + skt2(i)))
      .collect()

    sketches.map { case (fid, sketch) => sketch }
  }
}
