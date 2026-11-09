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

package com.tencent.angel.spark.ml.util

import java.util.concurrent._
import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.optimization.{L1Updater, SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.spark.ml.util._
import scala.collection.mutable.ArrayBuffer
import com.tencent.angel.spark.ml.util.OneHot._
import org.apache.spark.rdd.user.{CoalescedRDD => PartitionGroupRDD}


/**
 * :: DeveloperApi ::
 * The alternating direction method of multipliers (ADMM) is an algorithm that solves
 * convex optimization problems by breaking them into smaller pieces, each of which
 * are then easier to handle. It has recently found wide application in a number of areas.
 *
 * Class used to solove an optimization problem using ADMM. This is an implementation of
 * consensus form ADMM. This class can solve the optimization problems which can be described
 * as follow.
 * minimize \sum f_i(x_i) + g(z)
 *                      s.t. x_i - z = 0
 * where f_i(x) is the loss function for i_th block of training data, x_i is local variable,
 * z is the global variable. g(z) is an regularization which can be L1 or L2 norm. x_i - z = 0
 * is consistency or consensus constrains.
 * As described in Boyd's paper, x_i, z and scaled dual variable u_i will be updated as follow.
 * x_i = argmin (f_i(x_i) + 1/2 * rho * (x_i - z + u_i)^2)
 * u_i = u_i + x_i - z
 * z = argmin (g(z) + 1/2 * N * rho * (z - w)^2)
 * where N is the number of training data blocks to optimize in parallel, rho is the augmented
 * Lagrangian parameter, w is the average of (x_i + u_i).
 * Reference: [[http://stanford.edu/~boyd/papers/admm_distr_stats.html]]
 *
 * @param gradient Gradient function to be used
 * @param updater  Updater to be used to update weights after every iteration
 */
@DeveloperApi
class ADMM(private var gradient: Gradient, private var updater: Updater){

  private var primalConvergenceTol = 1e-5
  private var dualConvergenceTol = 1e-5
  private var subModelNum = 20
  private var maxNumIterations = 20
  private var regParam = 0.0
  private var rho = 1.0
  private var testSet: RDD[(Double, OneHotVector)] = null
  private var metricsSummary: MetricsSummary = _

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
    require(rho > 0, s"Rho must be nonnegative but got $rho")
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
   * Set metric summary
   */
  def setMetricSummary(modelPath: String): this.type = {
    this.metricsSummary = MetricsSummary(modelPath)
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

  def optimize(data: RDD[(Double, OneHotVector)], initModel: DenseVector): (DenseVector, Array[Double]) = {
    ADMM.runADMM(
      data,
      testSet,
      initModel,
      metricsSummary,
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

  //  private case class State(points: Array[(Double, Vector)])
  private case class Model(x: DenseVector, u: DenseVector)

  private case class OneHotInstance(label: Double, features: OneHotVector)

  private val lbfgsMaxIter = 5
  private val numCorrection = 7
  private val lbfgsConvergenceTol = 1e-9

  /**
   * Run Alternating Direction Method of Multipliers(ADMM) in parallel.
   *
   * @param data             - Input data for ADMM. RDD of the set of data examples, each of
   *                         the form (label, [feature values]).
   * @param initModel        - Initial model weight.
   * @param numSubModel      - Number of training data block to be split for parallel training.
   * @param gradient         - Gradient object (used to compute the gradient of the loss function of
   *                         one single data example).
   * @param updater          - Updater function to actually perform a gradient step in a given direction.
   * @param regParam         - Regularization parameter.
   * @param rho              - Augmented Lagrangian parameter.
   * @param maxNumIterations - Maximal number of iterations that ADMM can be run.
   * @param primalTol        - Primal convergence tolerance.
   * @param dualTol          - Dual convergence tolerance.
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */

  def runADMM(
               data: RDD[(Double, OneHotVector)],
               testSet: RDD[(Double, OneHotVector)],
               initModel: DenseVector,
               summary: MetricsSummary,
               numSubModel: Int,
               gradient: Gradient,
               updater: Updater,
               regParam: Double,
               rho: Double,
               maxNumIterations: Int = 20,
               primalTol: Double = 1e-5,
               dualTol: Double = 1e-5): (DenseVector, Array[Double]) = {

    var (states, subModels) = initialize(data, initModel, numSubModel)

    var lastZ: DenseVector = null
    var zModel = initModel
    var zBC = data.context.broadcast(zModel)
    val history = new ArrayBuffer[Double]()
    history += getGlobalLoss(states, zBC, gradient, updater, regParam)
    LogUtils.logTime(s"iter: 0 loss: ${history.last}")

    var numIter = 1
    do {
      LogUtils.logTime(s"iteration: $numIter begin...")
      val featNum = zModel.size

      val newSubModels = new PartitionGroupRDD[OneHotInstance, Model, Model](states, subModels, numSubModel) ({
        case (iterParts, iterModel) =>
          val (id, subModel) = iterModel.next()
          require(!iterModel.hasNext, s"zipPartitions failed, 1 partition has ${iterModel.length + 1} model")

          // update u_i (u_i = u_i + x_i - z)
          BLAS.axpy(1, subModel.x, subModel.u)
          BLAS.axpy(-1, zBC.value, subModel.u)

          // update x
          val parts = iterParts.toArray.map(iter => iter.toArray)
          val newX = xUpdate(id, numIter, parts, subModel, zBC.value, gradient, rho)

          // return new model
          Iterator.single(Tuple2(id, Model(newX, subModel.u)))
      }).persist(StorageLevel.MEMORY_ONLY)


      // use RDD.count to trigger the calculation of newSubModels
      newSubModels.count()
      subModels.unpersist()
      subModels = newSubModels

      // all reduce x + u
      LogUtils.logTime(s"iteration: $numIter update w")
      val w = wUpdate(subModels, featNum, numSubModel)

      // update z
      LogUtils.logTime(s"iteration: $numIter update z")
      lastZ = zModel
      zModel = zUpdate(numIter, featNum, numSubModel, w, updater, regParam, rho)
      zBC.unpersist()
      zBC = data.context.broadcast(zModel)

      // evaluate and loss
      val testAUC = if (testSet != null && testSet.count() > 0) evaluate(testSet, zBC) else 0.0
      val trainAUC = evaluate(data, zBC)
      LogUtils.logTime(s"iter: $numIter train auc: $trainAUC test auc: $testAUC")
      history += getGlobalLoss(states, zBC, gradient, updater, regParam)
      LogUtils.logTime(s"iter: $numIter loss: ${history.last}")

      // log metrics to summary
      summary.logMetrics("train auc", numIter, trainAUC)
      summary.logMetrics("test auc", numIter, testAUC)
      summary.logMetrics("loss", numIter, history.last)
      numIter += 1
    } while (numIter <= maxNumIterations &&
      !isConverged(subModels, lastZ, zBC, rho, primalTol, dualTol))
    LogUtils.logTime(s"global loss history: ${history.mkString(" ")}")
    (zModel, history.toArray)
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
      .mapPartitions({ iter =>
        Thread.sleep(30 * 1000)
        iter
      }, true).persist(StorageLevel.MEMORY_ONLY)

    LogUtils.logTime(s"data partition count: ${states.count()}")

    val subModels = data.context.parallelize(0 until numSubModels)
      .map(id => (id, id))
      .partitionBy(new HashPartitioner(numSubModels))
      .mapPartitions(iter =>
        iter.map(ids => (ids._1, Model(zModel.value, zModel.value))),
        preservesPartitioning = true
      ).persist(StorageLevel.MEMORY_ONLY)
    val modelCount = subModels.count()
    LogUtils.logTime(s"subModel count: $modelCount")

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
    LogUtils.logTime(s"iteration: $numIter, subModel id: $id lbfgs loss history: ${lbfgsHistory.mkString(" ")}")
    val weight = new DenseVector(summary.x.toArray)
    weight
  }

  private def wUpdate(subModels: RDD[(Int, Model)], featNum: Int, subModelNum: Int): DenseVector = {
    val splitSize = 1000
    val wSlices = subModels.flatMap { case (_, model) =>
      val subW = (0 until featNum).map { index =>
        model.x(index) + model.u(index)
      }.toArray
      subW.sliding(splitSize, splitSize)
        .zipWithIndex
        .map { case (arr, index) => (index, arr) }
    }.groupByKey()
      .map { case (index, iter) =>
        val arrSum = iter.reduce((arr1, arr2) => arr1.indices.map(i => arr1(i) + arr2(i)).toArray)
        (index, arrSum)
      }.collectAsMap()
    math.ceil(featNum.toDouble / splitSize)

    require(math.ceil(featNum.toDouble / splitSize) == wSlices.size)

    val w = new Array[Double](featNum)
    (0 until wSlices.size).foreach { index =>
      wSlices.apply(index).zipWithIndex
        .foreach { case (feat, i) => w(i + index * splitSize) = feat }
    }

    new DenseVector(w)
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
                       featNum: Int,
                       modelNum: Int,
                       w: Vector,
                       updater: Updater,
                       regParam: Double,
                       rho: Double): DenseVector = {
    // z update
    val newZ = updater match {
      case u: L1Updater =>
        val kappa = regParam / (rho * modelNum)
        val z = (0 until featNum).map { case i =>
          val wi = w(i) / modelNum
          shrinkage(wi, kappa)
        }
        Vectors.dense(z.toArray)
      case u: SquaredL2Updater =>
        val factor = (modelNum * rho) / (2 * regParam + modelNum * rho)
        val z = Vectors.zeros(featNum)
        BLAS.axpy(factor / modelNum, w, z)
        z
      case u: SimpleUpdater =>
        val z = Vectors.zeros(featNum)
        BLAS.axpy(1.0 / modelNum, w, z)
        z
    }

    LogUtils.logTime(s"iteration: $numIter model nonzero rate: ${newZ.numNonzeros.toDouble / featNum}")
    newZ.toDense
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
                             zBC: Broadcast[DenseVector],
                             gradient: Gradient,
                             updater: Updater,
                             regParam: Double): Double = {
    val featNum = zBC.value.size

    // only for logistic regression, without using gradient.
    val dataLoss = states.map { case (id, point) =>
      val label = point.label
      val feature = point.features
      gradient.compute(feature, label, zBC.value)
    }.mean()

    val regVal = updater.compute(zBC.value, Vectors.zeros(featNum), 0, 1, regParam)._2
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
                           lastZ: DenseVector,
                           zBC: Broadcast[DenseVector],
                           rho: Double,
                           primalTol: Double,
                           dualTol: Double): Boolean = {

    val featNum = zBC.value.size
    val t = models.map { case (_, subModel) =>
      (0 until featNum).map { case i =>
        math.pow(subModel.x(i) - zBC.value(i), 2)
      }.sum
    }.sum / models.count()

    val zModel = zBC.value.copy
    BLAS.axpy(-1, lastZ, zModel)
    val temp = zModel.toArray.map(fn => fn * fn).sum
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
                         data: Array[Array[(Int, OneHotInstance)]],
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
    val nThreads = 5

    class CalThread(iter: Array[(Int, OneHotInstance)], mlWeight: DenseVector, featNum: Int) extends Runnable {
      override def run(): Unit = {
        val countSum = iter.length
        var lossSum = 0.0
        val gradSum = Vectors.zeros(featNum).toDense

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
      val threadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable])
      //val threadPool = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])
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
        Thread.sleep(100)
      }

      BLAS.axpy(1.0 / gradientAndLoss.countSum, gradientAndLoss.gradSum, temp)

      val loss = gradientAndLoss.lossSum / gradientAndLoss.countSum + regLoss
      println(s"repeat: $repeatTime loss: $loss(${gradientAndLoss.countSum}, ${gradientAndLoss.lossSum}, $regLoss), " +
        s"gradient nonZero: ${temp.numNonzeros}")

      (loss, new BDV(temp.values))
    }
  }

  def evaluate(dataSet: RDD[(Double, OneHotVector)], modelBC: Broadcast[DenseVector]): Double = {
    val scoreAndLabel = dataSet.map { case (label, feature) =>
      val raw = OneHot.dot(feature, modelBC.value)
      val prob = 1.0 / (1.0 + math.exp(-1 * raw))
      (prob, label)
    }

    val bcMetric = new BinaryClassificationMetrics(scoreAndLabel)
    bcMetric.areaUnderROC()
  }
}


