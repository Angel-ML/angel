/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.angel.spark.ml.classification

import java.util.Random

import scala.language.implicitConversions
import scala.math._

import com.tencent.angel.spark.graphx._
import com.tencent.angel.spark.graphx.impl.GraphImpl
import com.tencent.angel.spark.graphx.util.Logging
import com.tencent.angel.spark.ml.classification.LogisticRegression._

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{DenseVector => SDV, SparseVector => SSV, Vector => SV}

abstract class LogisticRegression(
  @transient val dataSet: Graph[VD, ED],
  var stepSize: Double,
  var regParam: Double,
  var useAdaGrad: Boolean,
  @transient var storageLevel: StorageLevel) extends Serializable with Logging {

  @transient protected var innerIter = 1
  @transient protected var checkpointInterval = 10

  def setStepSize(stepSize: Double): this.type = {
    this.stepSize = stepSize
    this
  }

  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  def setAdaGrad(useAdaGrad: Boolean): this.type = {
    this.useAdaGrad = useAdaGrad
    this
  }

  def setCheckpointInterval(interval: Int): this.type = {
    this.checkpointInterval = interval
    this
  }

  @transient protected var margin: RDD[(VertexId, VD)] = null
  @transient protected var gradientSum: RDD[(VertexId, VD)] = null
  @transient protected var gradient: RDD[(VertexId, VD)] = null
  @transient protected var edges = dataSet.edges
  if (edges.sparkContext.getCheckpointDir.isDefined) {
    edges.checkpoint()
    dataSet.vertices.checkpoint()
    edges.count()
  }
  dataSet.persist(storageLevel)

  val numFeatures: Long = features.count()
  val numSamples: Long = samples.count()

  private[ml] def samples: RDD[(VertexId, VD)] = {
    dataSet.vertices.filter(t => isSampleId(t._1))
  }

  private[ml] def features: RDD[(VertexId, VD)] = {
    dataSet.vertices.filter(t => isFeatureId(t._1))
  }

  def run(iterations: Int): Unit = {
    for (iter <- 1 to iterations) {
      logInfo(s"Start train (Iteration $iter/$iterations)")
      val startedAt = System.nanoTime()
      margin = forward(innerIter)
      gradient = backward(margin, innerIter)
      gradient = updateGradientSum(gradient, innerIter)
      val tis = if (useAdaGrad) stepSize else stepSize / sqrt(innerIter)
      val l1tis = stepSize / sqrt(innerIter)
      updateWeight(gradient, innerIter, tis, l1tis)
      val elapsedSeconds = (System.nanoTime() - startedAt) / 1e9
      println(s"train (Iteration $iter/$iterations) loss:              ${loss(margin)}")
      logInfo(s"End  train (Iteration $iter/$iterations) takes:         $elapsedSeconds")
      unpersistVertices()
      innerIter += 1
    }
  }

  protected[ml] def forward(iter: Int): RDD[(VertexId, VD)]

  protected[ml] def backward(q: RDD[(VertexId, VD)], iter: Int): RDD[(VertexId, VD)]

  protected def loss(q: RDD[(VertexId, VD)]): Double

  def saveModel(numFeatures: Int = -1): GeneralizedLinearModel = {
    val features = this.features.map(t => (vertexId2FeatureId(t._1), t._2))

    val len = if (numFeatures < 0) features.map(_._1).max().toInt + 1 else numFeatures
    val featureData = new Array[Double](len)
    features.toLocalIterator.foreach { case (index, value) =>
      featureData(index.toInt) = value
    }
    new LogisticRegressionModel(new SDV(featureData), 0.0, len, 2).clearThreshold()
  }

  protected def updateGradientSum(gradient: RDD[(VertexId, VD)], iter: Int): RDD[(VertexId, VD)] = {
    if (gradient.getStorageLevel == StorageLevel.NONE) {
      gradient.setName(s"gradient-$iter").persist(storageLevel)
    }
    if (useAdaGrad) {
      val delta = adaGrad(gradientSum, gradient, 1e-4, 1.0)
      checkpointGradientSum(delta)
      delta.setName(s"delta-$iter").persist(storageLevel).count()

      gradient.unpersist(blocking = false)
      val newGradient = delta.mapValues(_.head).setName(s"gradient-$iter").persist(storageLevel)
      newGradient.count()

      if (gradientSum != null) gradientSum.unpersist(blocking = false)
      gradientSum = delta.mapValues(_.last).setName(s"deltaSum-$iter").persist(storageLevel)
      gradientSum.count()
      delta.unpersist(blocking = false)
      newGradient
    } else {
      gradient
    }
  }

  // Updater for L1 regularized problems
  protected[ml] def updateWeight(
    delta: RDD[(VertexId, VD)],
    iter: Int,
    thisIterStepSize: Double,
    thisIterL1StepSize: Double): Unit = {
    val newW = dataSet.vertices.join(delta).filter(e => isFeatureId(e._1)).map { case (vid, (attr, gard)) =>
      var weight = attr
      weight -= thisIterStepSize * gard
      if (regParam > 0.0 && weight != 0.0) {
        val shrinkageVal = regParam * thisIterL1StepSize
        weight = signum(weight) * max(0.0, abs(weight) - shrinkageVal)
      }
      assert(!weight.isNaN)
      (vid, weight)
    }.persist(storageLevel)
    newW.count()
    dataSet.vertices.updateValues(newW)
    newW.unpersist()
  }

  protected def adaGrad(
    gradientSum: RDD[(VertexId, VD)],
    gradient: RDD[(VertexId, VD)],
    epsilon: Double,
    rho: Double): RDD[(VertexId, Array[Double])] = {
    val delta = if (gradientSum == null) {
      gradient.mapValues(t => 0.0)
    } else {
      gradientSum
    }
    delta.join(gradient).map {
      case (vid, (gradSum, grad)) =>
        val newGradSum = gradSum * rho + pow(grad, 2)
        val newGrad = grad / (epsilon + sqrt(newGradSum))
        (vid, Array(newGrad, newGradSum))
    }
  }

  protected def checkpointGradientSum(delta: RDD[(VertexId, Array[Double])]): Unit = {
    val sc = delta.sparkContext
    if (innerIter % checkpointInterval == 0 && sc.getCheckpointDir.isDefined) {
      delta.checkpoint()
    }
  }


  protected def unpersistVertices(): Unit = {
    if (gradient != null) gradient.unpersist(blocking = false)
    if (margin != null) margin.unpersist(blocking = false)
  }
}

// Modified Iterative Scaling, the paper:
// A comparison of numerical optimizers for logistic regression
// http://research.microsoft.com/en-us/um/people/minka/papers/logreg/minka-logreg.pdf
class LogisticRegressionMIS(
  dataSet_ : Graph[VD, ED],
  stepSize_ : Double,
  regParam_ : Double,
  useAdaGrad_ : Boolean,
  storageLevel_ : StorageLevel) extends LogisticRegression(dataSet_, stepSize_, regParam_,
  useAdaGrad_, storageLevel_) {
  def this(
    input: RDD[(VertexId, LabeledPoint)],
    stepSize: Double = 1e-4,
    regParam: Double = 0.0,
    useAdaGrad: Boolean = false,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    this(initializeDataSet(input, storageLevel), stepSize, regParam, useAdaGrad, storageLevel)
  }

  @transient private var qWithLabel: VertexRDD[VD] = null
  private var epsilon = 1e-4

  def setEpsilon(eps: Double): this.type = {
    epsilon = eps
    this
  }

  override protected[ml] def backward(z: RDD[(VertexId, VD)], iter: Int): RDD[(VertexId, VD)] = {
    val q = z.mapValues { z =>
      val q = 1.0 / (1.0 + exp(z))
      // if (q.isInfinite || q.isNaN || q == 0.0) println(z)
      assert(q != 0.0)
      q
    }
    val vertices = dataSet.vertices
    qWithLabel = vertices.copy(withValues = false).updateValues(vertices.join(q).filter(
      t => isSampleId(t._1)).map { case (vid, (label, q)) =>
      (vid, if (label > 0) q else -q)
    })
    val newDataSet = GraphImpl.fromExistingRDDs(qWithLabel, dataSet.edges)
    val grads = newDataSet.mapReduceTriplets[Array[Double]](ctx => {
      // val sampleId = ctx.dstId
      // val featureId = ctx.srcId
      val x = ctx.attr
      val qs = ctx.dstAttr
      val q = qs * x
      assert(q != 0.0)
      val mu = if (q > 0.0) {
        Array(q, 0.0)
      } else {
        Array(0.0, -q)
      }
      Iterator((ctx.srcId, mu))
    }, (a, b) => Array(a(0) + b(0), a(1) + b(1)), TripletFields.Dst).map { case (vid, mu) =>
      // TODO: 0.0 right?
      val grad = if (epsilon == 0.0) {
        if (mu(0) == 0.0 || mu(1) == 0.0) 0.0 else math.log(mu(0) / mu(1))
      } else {
        math.log((mu(0) + epsilon) / (mu(1) + epsilon))
      }
      (vid, -grad)
    }.setName(s"gradient-$iter").persist(storageLevel)
    grads.localCheckpoint()
    grads.count()
    grads
  }

  override protected[ml] def forward(iter: Int): RDD[(VertexId, VD)] = {
    dataSet.mapReduceTriplets[Double](edge => {
      // val sampleId = edge.dstId
      // val featureId = edge.srcId
      val x = edge.attr
      val w = edge.srcAttr
      val y = edge.dstAttr
      val z = y * w * x
      // if(z==0) println(s"$y $w $x")
      Iterator((edge.dstId, z))
    }, _ + _, TripletFields.All).setName(s"q-$iter").persist(storageLevel)
  }

  override protected[ml] def loss(q: RDD[(VertexId, VD)]): Double = {
    dataSet.vertices.join(q).filter(t => isSampleId(t._1)).map { case (_, (y, z)) =>
      if (y > 0.0) {
        log1pExp(-z)
      } else {
        log1pExp(z) - z
      }
    }.reduce(_ + _) / numSamples
  }

  override protected def unpersistVertices(): Unit = {
    if (qWithLabel != null) {
      qWithLabel.unpersist(blocking = false)
      qWithLabel.destroy(blocking = false)
    }
    super.unpersistVertices()
  }
}

object LogisticRegression {
  private[ml] type ED = Double
  private[ml] type VD = Double

  /**
   * Modified Iterative Scaling
   * The referenced paper:
   * A comparison of numerical optimizers for logistic regression
   * http://research.microsoft.com/en-us/um/people/minka/papers/logreg/minka-logreg.pdf
   *
   * @param input         training data, feature value must >= 0, label is either 0 or 1 (binary classification)
   * @param numIterations maximum number of iterations
   * @param stepSize      step size, recommend to be in value range 0.1 - 1.0
   * @param regParam      L1 Regularization
   * @param epsilon       smoothing parameter, 1e-4 - 1e-6
   * @param useAdaGrad    adaptive step size, recommend to be true
   * @param storageLevel  recommendation configuration: MEMORY_AND_DISK for small/middle-scale training data,
   *                      and DISK_ONLY for super-large-scale data
   */
  def trainMIS(
    input: RDD[(Long, LabeledPoint)],
    psMaster: String,
    numIterations: Int,
    stepSize: Double,
    regParam: Double,
    epsilon: Double = 1e-3,
    useAdaGrad: Boolean = false,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): GeneralizedLinearModel = {
    val data = input.map { case (id, LabeledPoint(label, features)) =>
      assert(id >= 0.0, s"sampleId $id less than 0")
      val newLabel = if (label > 0.0) 1.0 else -1.0
      features.activeValuesIterator.foreach(t => assert(t >= 0.0, s"feature $t less than 0"))
      (id, LabeledPoint(newLabel, features))
    }
    val lr = new LogisticRegressionMIS(data, stepSize, regParam, useAdaGrad, storageLevel)
    lr.setEpsilon(epsilon).run(numIterations)
    lr.saveModel()
  }

  private[ml] def initializeDataSet(
    input: RDD[(VertexId, LabeledPoint)],
    storageLevel: StorageLevel): Graph[VD, ED] = {
    val edges = input.flatMap { case (sampleId, labelPoint) =>
      labelPoint.features.activeIterator.filter(_._2 != 0.0).map { case (featureId, value) =>
        Edge(featureId2VertexId(featureId), sampleId2VertexId(sampleId), value)
      }
    }.persist(storageLevel)

    val vertices = (input.map { case (sampleId, labelPoint) =>
      // sample point
      (sampleId2VertexId(sampleId), labelPoint.label)
    } ++ edges.map(_.srcId).distinct().map { featureId =>
      // parameter point
      val parms = random.nextGaussian() * 1e-2
      (featureId2VertexId(featureId), parms)
    }).persist(storageLevel)
    val newDataSet = GraphImpl.fromExistingRDDs(vertices, edges)
    newDataSet.persist(storageLevel)
    newDataSet.vertices.count()
    newDataSet.edges.count()

    edges.unpersist(blocking = false)
    vertices.unpersist(blocking = false)
    newDataSet
  }

  @inline private[ml] def vertexId2SampleId(id: VertexId): Long = {
    id / 2
  }

  @inline private[ml] def vertexId2FeatureId(id: VertexId): Long = {
    id / 2
  }

  @inline private[ml] def sampleId2VertexId(id: Long): VertexId = {
    id * 2 + 1
  }

  @inline private[ml] def isSampleId(id: VertexId): Boolean = {
    id % 2 == 1
  }

  @inline private[ml] def featureId2VertexId(id: Long): VertexId = {
    id * 2
  }

  @inline private[ml] def isFeatureId(id: VertexId): Boolean = {
    id % 2 == 0
  }

  implicit def toBreeze(sv: SV): BV[Double] = {
    sv match {
      case SDV(data) =>
        new BDV(data)
      case SSV(size, indices, values) =>
        new BSV(indices, values, size)
    }
  }

  implicit def fromBreeze(breezeVector: BV[Double]): SV = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new SDV(v.data)
        } else {
          new SDV(v.toArray) // Can't use underlying array directly, so make a new one
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SSV(v.length, v.index, v.data)
        } else {
          new SSV(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  val random = new Random()

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}
