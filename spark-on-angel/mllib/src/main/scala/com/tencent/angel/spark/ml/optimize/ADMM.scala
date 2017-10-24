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
 *
 */

package com.tencent.angel.spark.ml.optimize

import java.util.concurrent._

import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.ml.common.OneHot.OneHotVector
import com.tencent.angel.spark.ml.common.{BLAS, Gradient, OneHot}
import com.tencent.angel.spark.ml.psf.ADMMZUpdater
import org.apache.spark.HashPartitioner
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.optimization.{L1Updater, Updater}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.user.CoalescedRDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * :: DeveloperApi ::
 * The alternating direction method of multipliers (ADMM) is an algorithm that solves
 * convex optimization problems by breaking them into smaller pieces, each of which
 * are then easier to handle. It has recently found wide application in a number of areas.
 *
 * Class used to solove an optimization problem using ADMM. This is an implementation of
 * consensus form ADMM. This class can solve the optimization problems which can be described
 * as follow.
 *                minimize \sum f_i(x_i) + g(z)
 *                      s.t. x_i - z = 0
 * where f_i(x) is the loss function for i_th block of training data, x_i is local variable,
 * z is the global variable. g(z) is an regularization which can be L1 or L2 norm. x_i - z = 0
 * is consistency or consensus constrains.
 * As described in Boyd's paper, x_i, z and scaled dual variable u_i will be updated as follow.
 *              x_i = argmin (f_i(x_i) + 1/2 * rho * (x_i - z + u_i)^2)
 *              u_i = u_i + x_i - z
 *                z = argmin (g(z) + 1/2 * N * rho * (z - w)^2)
 * where N is the number of training data blocks to optimize in parallel, rho is the augmented
 * Lagrangian parameter, w is the average of (x_i + u_i).
 * Reference: [[http://stanford.edu/~boyd/papers/admm_distr_stats.html]]
 *
 * @param gradient Gradient function to be used
 * @param updater Updater to be used to update weights after every iteration
 */
@DeveloperApi
class ADMM(private var gradient: Gradient, private var updater: Updater) {

  private var primalConvergenceTol = 1e-5
  private var dualConvergenceTol = 1e-5
  private var subModelNum = 20
  private var maxNumIterations = 20
  private var regParam = 0.0
  private var rho = 1.0
  private var testSet: RDD[(Double, OneHotVector)] = null

  /**
   * Set the maximal number of iterations for ADMM. Default 20.
   */
  def setNumIterations(iterNum: Int): this.type = {
    require(iterNum >= 0, s"Maximum of iterations must be nonnegative but got $iterNum")
    this.maxNumIterations = iterNum
    this
  }

  /**
   * Set the number of sub models to be trained in parallel. Default 20.
   */
  def setNumSubModels(num: Int): this.type = {
    require(num > 0, s"Number of sub models must be nonnegative but got $num")
    this.subModelNum = num
    this
  }

  /**
   * set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    require(regParam >= 0, s"Regularization parameter must be nonnegative but got $regParam")
    this.regParam = regParam
    this
  }

  /**
   * Set rho which is the augmented Lagrangian parameter.
   * kappa = regParam / (rho * numSubModels), if the absolute value of element in model
   * is less than kappa, this element will be assigned to zero.
   * So kappa should be less than 0.01 or 0.001.
   */
  def setRho(rho: Double): this.type = {
    require(rho >= 0, s"Rho must be nonnegative but got $rho")
    this.rho = rho
    this
  }

  /**
   * Set the primal convergence tolerance of iterations.
   *
   */
  def setPrimalTol(tolenrance: Double): this.type = {
    require(tolenrance >= 0,
      s"Primal convergence tolerance must be nonnegative but got $tolenrance")
    this.primalConvergenceTol = tolenrance
    this
  }

  /**
   * Set the dual convergence tolerance of iterations.
   */
  def setDualTol(tolenrance: Double): this.type = {
    require(tolenrance >= 0,
      s"Dual convergence tolerance must be nonnegative but got $tolenrance")
    this.dualConvergenceTol = tolenrance
    this
  }

  /**
   * Set test set
   */
  def setTestSet(testSet: RDD[(Double, OneHotVector)]): this.type = {
    this.testSet = testSet
    this
  }

  /**
   * set the gradient function (of the loss fuction of one single data example)
   * to be used for ADMM
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }

  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  def optimize(data: DataFrame, initModel: DenseVector): (DenseVector, Array[Double]) = {
    val dataRDD = data.rdd.map { case Row(label: Double, feature: Array[Int]) =>
      Tuple2(label, feature)
    }
    optimize(dataRDD, initModel)
  }

  def optimize(data: RDD[(Double, OneHotVector)],
               initModel: DenseVector): (DenseVector, Array[Double]) = {
    ADMM.runADMM(data, testSet, initModel)(
      subModelNum,
      gradient,
      updater,
      regParam,
      rho,
      maxNumIterations,
      primalConvergenceTol,
      dualConvergenceTol)
  }
}

/**
 * :: DeveloperApi ::
 * Top-level method to run ADMM
 */
@DeveloperApi
object ADMM {

  private case class Model(x: DenseVector, u: DenseVector)
  private case class OneHotInstance(label: Double, features: OneHotVector)

  private val lbfgsMaxIter = 5
  private val numCorrection = 7
  private val lbfgsConvergenceTol = 1e-9

  /**
   * Run Alternating Direction Method of Multipliers(ADMM) in parallel.
    *
    * @param data - Input data for ADMM. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param initModel - Initial model weight.
   * @param numSubModel - Number of training data block to be split for parallel training.
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example).
   * @param updater - Updater function to actually perform a gradient step in a given direction.
   * @param regParam - Regularization parameter.
   * @param rho - Augmented Lagrangian parameter.
   * @param maxNumIterations - Maximal number of iterations that ADMM can be run.
   * @param primalTol - Primal convergence tolerance.
   * @param dualTol - Dual convergence tolerance.
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runADMM[SparkV <: DenseVector, AngelV <: PSVector](data: RDD[(Double, OneHotVector)], testSet: RDD[(Double, OneHotVector)], initModel: SparkV)
             (numSubModel: Int,
              gradient: Gradient,
              updater: Updater,
              regParam: Double,
              rho: Double,
              maxNumIterations: Int = 20,
              primalTol: Double = 1e-5,
              dualTol: Double = 1e-5): (SparkV, Array[Double]) = {

    var (states, subModels) = initialize(data, initModel, numSubModel)

    var lastZ: PSVector = null
    var zPS: PSVector = PSVector.dense(initModel.size)

    val history = new ArrayBuffer[Double]()
    history += getGlobalLoss(states, zPS, gradient, updater, regParam)
    println(s"iter: 0 loss: ${history.last}")

    var numIter = 1
    do {
      println(s"iteration: $numIter begin...")

      val newSubModels = new CoalescedRDD[OneHotInstance, Model, Model](states,
        subModels, numSubModel) ({
        case (iterParts, iterModel) =>
          val (id, subModel) = iterModel.next()
          require(!iterModel.hasNext,
            s"zipPartitions failed, 1 partition has ${iterModel.length + 1} model")
          val localZ = new DenseVector(zPS.pull())

          // update u_i (u_i = u_i + x_i - z)
          BLAS.axpy(1, subModel.x, subModel.u)
          BLAS.axpy(-1, localZ, subModel.u)

          // update x
          val bufferIter = iterParts.toArray.map (iter => iter.toArray)
          val newX = xUpdate(id, numIter, bufferIter, subModel, localZ, gradient, rho)

          // return new model
          Iterator.single(Tuple2(id, Model(newX, subModel.u)))
      }).persist(StorageLevel.MEMORY_ONLY)

      // use RDD.count to trigger the calculation of newSubModels
      newSubModels.count()
      subModels.unpersist()
      subModels = newSubModels

      // all reduce x + u
      println(s"iteration: $numIter update w")

      val wSum = PSVector.duplicate(zPS)
      val w = wUpdate(wSum, subModels, numSubModel)

      // update z
      println(s"iteration: $numIter update z")
      lastZ = zPS.toBreeze.copy.component
      zPS = zUpdate(numIter, numSubModel, w, updater, regParam, rho)

      // evaluate and loss
      val testAUC = if (testSet != null && testSet.count() > 0) evaluate(testSet, zPS) else 0.0
      val trainAUC = evaluateState(states, zPS)
      println(s"iter: $numIter train auc: $trainAUC test auc: $testAUC")
      history += getGlobalLoss(states, zPS, gradient, updater, regParam)
      println(s"iter: $numIter loss: ${history.last}")

      // log metrics to summary
      println(s"iterator: $numIter loss: ${history.last} train auc: $trainAUC test auc: $testAUC")

      numIter += 1
    } while (numIter <= maxNumIterations &&
      !isConverged(subModels, lastZ, zPS, rho, primalTol, dualTol))
    println(s"global loss history: ${history.mkString(" ")}")

    val finalModel = new DenseVector(zPS.pull()).asInstanceOf[SparkV]
    (finalModel, history.toArray)
  }

  /**
   * Separate the whole data into $subModelNum parts which store as States.
   * and initialize corresponding Models.
   */
  private def initialize(
      data: RDD[(Double, OneHotVector)],
      initModel: DenseVector,
      numSubModels: Int): (RDD[(Int, OneHotInstance)], RDD[(Int, Model)]) = {

    val zModel = data.context.broadcast(initModel)
    val partitionNum = data.partitions.length

    val states = data.map { case (label, feature) =>
      val instance = OneHotInstance(label, feature)
      (instance.hashCode(), instance)
    }.partitionBy(new HashPartitioner(partitionNum))
    .persist(StorageLevel.MEMORY_ONLY)

    println(s"data partition count: ${states.count()}")

    val subModels = data.context.parallelize(0 until numSubModels)
      .map(id => (id, id))
      .partitionBy(new HashPartitioner(numSubModels))
      .mapPartitions(iter =>
        iter.map(ids => (ids._1, Model(zModel.value, zModel.value))),
        preservesPartitioning = true
      ).persist(StorageLevel.MEMORY_ONLY)
    val modelCount = subModels.count()
    println(s"subModel count: $modelCount")

    (states, subModels)
  }

  /**
   * Optimize x_i with Breeze's L-BFGS,
   * where x_i = argmin (f_i(x_i) + 1/2 * rho * (x_i - z + u_i) * (x_i - z + u_i).
   */
  private def xUpdate(
      id: Long,
      numIter: Int,
      iterParts: Array[Array[(Int, OneHotInstance)]],
      subModel: Model,
      z: DenseVector,
      gradient: Gradient,
      rho: Double): DenseVector = {
    val costFun = new CostFun(id, iterParts, gradient, subModel.u, z, rho)
    val lbfgs = new BreezeLBFGS[BV[Double]](lbfgsMaxIter, numCorrection, lbfgsConvergenceTol)

    val summaries = lbfgs.iterations(new CachedDiffFunction(costFun), new BDV(subModel.x.values))

    val lbfgsHistory = new ArrayBuffer[Double]()
    var summary = summaries.next()
    while (summaries.hasNext) {
      lbfgsHistory += summary.value
      summary = summaries.next()
    }
    lbfgsHistory += summary.value
    println(s"iteration: $numIter, subModel id: $id " +
      s"lbfgs loss history: ${lbfgsHistory.mkString(" ")}")

    val weight = new DenseVector(summary.x.toArray)
    weight
  }

  private def wUpdate(wSum: PSVector,
                      subModels: RDD[(Int, Model)],
                      subModelNum: Int): PSVector = {
    subModels.foreach { case (id, model) =>
      println(s"id: $id x nnz: ${model.x.numNonzeros} u nnz: ${model.u.numNonzeros}")}

    def sumArray(x: Array[Double], y: Array[Double]): Array[Double] = {
      require(x.length == y.length)
      x.indices.toArray.map(index => x(index) + y(index))
    }

    subModels.mapPartitions { iter =>
      val sum = iter.map { case (index, model) =>
        sumArray(model.x.toArray, model.u.toArray)
      }.reduce((x1, x2) => sumArray(x1, x2))
      wSum.toCache.increment(sum)
      Iterator.empty
    }.count()

    val wAver = wSum.toBreeze :* (1.0 / subModelNum)
    wAver.component
  }

  /**
   * Optimize z, where z = argmin (g(z) + 1/2 * N * rho * (z - w)^2)
   * and w = (1 / N) * \sum {x_i + u_i}.
   * If the objective function is L1 regularized problem, it means g(z) = lambda * ||z||,
   * then z = Shrinkage(w, kappa), where kappa = lambda / rho * N.
   * If the objective function is L2 regularized problem, it means g(z) = lambda * ||z||^2,
   * then z = (N * rho) / (2 * lambda + N * rho) * w
   */
  private def zUpdate(
      numIter: Int,
      modelNum: Int,
      w: PSVector,
      updater: Updater,
      regParam: Double,
      rho: Double): PSVector = {

    val newZ = updater match {
      case u: L1Updater =>
        val kappa = regParam / (rho * modelNum)
        w.toBreeze.map(new ADMMZUpdater(kappa))
    }
    newZ.component
  }

  /**
   * The corresponding proximal operator for the L1 norm is the soft-threshold
   * function. That is, each value is shrunk towards 0 by kappa value.
   *
   * if x < kappa or x > -kappa, return 0
   * if x > kappa, return x - kappa
   * if x < -kappa, return x + kappa
   */
  private def shrinkage(x: Double, kappa: Double): Double = {
    math.max(0, x - kappa) - math.max(0, -x - kappa)
  }

  private def getGlobalLoss(
                             states: RDD[(Int, OneHotInstance)],
                             zPS: PSVector,
                             gradient: Gradient,
                             updater: Updater,
                             regParam: Double): Double = {
    val localZ = new DenseVector(zPS.pull())
    val featNum = localZ.size

    // only for logistic regression, without using gradient.
    val dataLoss = states.map { case (id, point) =>
      val label = point.label
      val feature = point.features
      gradient.compute(feature, label, localZ)
    }.mean()

    val regVal = updater.compute(localZ, Vectors.zeros(featNum), 0, 1, regParam)._2
    dataLoss + regVal
  }

  /**
   * If both primal residual and dual residual is less than the tolerance,
   * the algorithm is converged.
   * s = rho * (z - z)
   * r = x - z
   */
  private def isConverged(
                           models: RDD[(Int, Model)],
                           lastZ: PSVector,
                           zPS: PSVector,
                           rho: Double,
                           primalTol: Double,
                           dualTol: Double): Boolean = {

    val localLastZ = lastZ.pull()
    val localZ = zPS.pull()

    val featNum = localLastZ.length
    val t = models.map { case (_, subModel) =>
      (0 until featNum).map { i =>
        math.pow(subModel.x(i) - localZ(i), 2)
      }.sum
    }.sum / models.count()

    val temp = (0 until featNum).map { i =>
      (localZ(i) - localLastZ(i)) * (localZ(i) - localLastZ(i))
    }.sum

    val primalError = rho * temp
    val dualError = math.sqrt(t)
    (primalError <= primalTol) && (dualError <= dualTol)
  }

  /**
   * CostFun implements Breeze's DiffFunction[T], which returns the loss and gradient
   * at a particular point (weights). It's used in Breeze's convex optimization routines.
   */
  private class CostFun(
      id: Long,
      var data: Array[Array[(Int, OneHotInstance)]],
      gradient: Gradient,
      u: Vector,
      z: Vector,
      rho: Double,
      var repeatTime: Int = 0) extends DiffFunction[BV[Double]] {

    case class GradientAndLoss(var gradSum: Vector, var lossSum: Double, var countSum: Int) {
      def add(grad: Vector, loss: Double, count: Int): Unit = {
        this.synchronized {
          BLAS.axpy(1.0, grad, gradSum)
          lossSum += loss
          countSum += count
        }
      }
    }

    var gradientAndLoss: GradientAndLoss = _
    class CalThread(iter: Array[(Int, OneHotInstance)],
                    mlWeight: DenseVector,
                    featNum: Int) extends Runnable {
      def run(): Unit = {
        val gradSum = new DenseVector(Array.fill(featNum)(0.0))
        var lossSum = 0.0
        val countSum = iter.length
        iter.foreach { case (index, instance) =>
            val l = gradient.compute(instance.features, instance.label, mlWeight, gradSum)
            lossSum += l
        }
        gradientAndLoss.add(gradSum, lossSum, countSum)
      }
    }

    override def calculate(weights: BV[Double]): (Double, BV[Double]) = {
      repeatTime += 1
      val n = weights.length
      val mlWeight = new DenseVector(weights.toArray)

      gradientAndLoss = GradientAndLoss(Vectors.zeros(n).toDense, 0.0, 0)
      val threadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
        new SynchronousQueue[Runnable])
      data.foreach { iter =>
        threadPool.execute(new CalThread(iter, mlWeight, n))
      }
      threadPool.shutdown()

      // temp = x - z + u
      val temp = mlWeight.copy
      BLAS.axpy(-1, z, temp)
      BLAS.axpy(1, u, temp)
      val regLoss = 0.5 * rho * BLAS.dot(temp, temp)

      // gradient = temp
      BLAS.scal(rho, temp)

      while (!threadPool.isTerminated) {
        Thread.sleep(1000)
        println(s"Await GradientCalculator Runnable to terminate")
      }

      BLAS.axpy(1.0 / gradientAndLoss.countSum, gradientAndLoss.gradSum, temp)

      val loss = gradientAndLoss.lossSum / gradientAndLoss.countSum + regLoss
      println(s"repeat: $repeatTime loss: $loss(${gradientAndLoss.countSum}," +
        s"${gradientAndLoss.lossSum}, $regLoss), gradient nonZero: ${temp.numNonzeros}")

      (loss, new BDV(temp.values))
    }
  }

  private def evaluate(dataSet: RDD[(Double, OneHotVector)], modelPS: PSVector): Double = {
    val scoreAndLabel = dataSet.map { case (label, feature) =>
      val raw = OneHot.dot(feature, new DenseVector(modelPS.pull()))
      val prob = 1.0 / (1.0 + math.exp(-1 * raw))
      (prob, label)
    }

    val bcMetric = new BinaryClassificationMetrics(scoreAndLabel)
    bcMetric.areaUnderROC()
  }

  private def evaluateState(states: RDD[(Int, OneHotInstance)], model: PSVector): Double = {
    val scoreAndLabel = states.mapPartitions { iter =>
      val thisModel = new DenseVector(model.pull())
      iter.map { case (id, instance) =>
        val raw = OneHot.dot(instance.features, thisModel)
        val prob = 1.0 / (1.0 + math.exp(-1 * raw))
        (prob, instance.label)
      }
    }

    val bcMetric = new BinaryClassificationMetrics(scoreAndLabel)
    bcMetric.areaUnderROC()
  }
}


