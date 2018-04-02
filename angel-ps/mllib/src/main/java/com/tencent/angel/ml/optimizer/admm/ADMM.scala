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

package com.tencent.angel.ml.optimizer.admm

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import breeze.linalg.{DenseVector, Vector}
import breeze.optimize.{CachedDiffFunction, DiffFunction}
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.classification.sparselr.SparseLRModel
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.{VectorType, TVector, TAbstractVector}
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.metric.GlobalMetrics
import com.tencent.angel.ml.conf.MLConf._
import com.tencent.angel.ml.utils.Maths
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.commons.logging.LogFactory

import scala.collection.mutable.ArrayBuffer

class ADMM {
  private var primalConvergenceTol = 1e-5
  private var dualConvergenceTol = 1e-5
  private var maxNumIterations = 20
  private var regParam = 0.1
  private var rho = 1.0
  private var N = 10

  def setNumIterations(iterNum: Int): this.type = {
    require(iterNum >= 0, s"Maximum of iterations must be nonnegative but got $iterNum")
    this.maxNumIterations = iterNum
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

  def setN(N: Int): this.type = {
    this.N = N
    this
  }
}

class LogisticGradient(numClasses: Int = 2) {
  type V = DenseDoubleVector

  def this() = this(2)

  def compute(data: TAbstractVector, label: Double, weights: V): Double = {
    numClasses match {
      case 2 =>
        val margin = -1.0 * weights.dot(data)
        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          log1pExp(margin)
        } else {
          log1pExp(margin) - margin
        }
      case _ =>
        throw new Exception("Logistic can not support multiClass")
    }
  }

  def compute(data: TAbstractVector,
              label: Double,
              weights: V,
              cumGradient: V): Double = {
    require(weights.size == cumGradient.size)
    numClasses match {
      case 2 =>
        val margin = -1.0 * weights.dot(data)
        val multiplier = (1.0 / (1.0 + math.exp(margin))) - label
        cumGradient.plusBy(data, multiplier)
        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          log1pExp(margin)
        } else {
          log1pExp(margin) - margin
        }
      case _ =>
        throw new Exception("Logistic can not support multiClass")
    }
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}


object ADMM {

  private val lbfgsMaxIter = 5
  private val numCorrection = 7
  private val lbfgsConvergenceTol = 1e-9

  private case class LocalModel(var x: Array[Double], u: Array[Double])

  private case class LocalIndex(localFeaNum: Int, localToGloabl: Array[Int], globalToLocal: Int2IntOpenHashMap)

  private val LOG = LogFactory.getLog(classOf[ADMM])

  type T = SparseDoubleVector

  def runADMM(data: DataBlock[LabeledData], model: SparseLRModel)
             (regParam: Double, rho: Double, N: Int, threadNum: Int,
              maxNumIterations: Int = 20, primalTol: Double = 1e-5, dualTol: Double = 1e-5)
             (implicit ctx: TaskContext, globalMetrics: GlobalMetrics): (Array[Double], T) = {

    val (localModel, localIndex) = initialize(data)
    val history = new ArrayBuffer[Double]()
    val z = TVector(model.indexRange.toInt, VectorType.T_DOUBLE_SPARSE).asInstanceOf[T]

    for (iter <- 1 until maxNumIterations) {
      val startTrainX = System.currentTimeMillis()
      updateUandX(z, data, localModel, localIndex, rho, threadNum)
      val trainXCost = System.currentTimeMillis() - startTrainX

      val startTrainZ = System.currentTimeMillis()
      updateW(model, localModel, localIndex)
      computeZ(z, model, regParam, rho, N)
      val trainZCost = System.currentTimeMillis() - startTrainZ
      LOG.info(s"epoch=$iter trainXCost=$trainXCost trainZCost=$trainZCost")

      val loss = calcLocalLoss(data, localModel)
      globalMetrics.metric(TRAIN_LOSS, loss)

      val precision = validate(data, z, localIndex, localModel)
      LOG.info(s"epoch=$iter global_loss=${loss} precision=$precision")

      model.t.clock(false)
      model.t.getRow(0)
      if (ctx.getTaskIndex == 0) model.clean()
      model.t.clock(false)
      model.t.getRow(0)

      ctx.incEpoch()
    }

    (history.toArray, z)
  }

  private def initialize(data: DataBlock[LabeledData]): (LocalModel, LocalIndex) = {
    val globalToLocal = new Int2IntOpenHashMap()
    val localToGlobal = new Int2IntOpenHashMap()
    var localFeatNum = 0

    // Reset the reader index for data samples
    data.resetReadIndex()
    var finish = false
    while (!finish) {
      // For each sample
      data.read() match {
        // If null, finish
        case null => finish = true
        // Else, compute the number of local features,
        // and map each global feature id to local feature
        case sample: LabeledData =>
          sample.getX match {
            // SparseDummyVector is allowed here
            case x: SparseDummyVector =>
              val len = x.getNonzero
              val indices = x.getIndices
              for (i <- 0 until len) {
                val fid = indices(i)
                if (!globalToLocal.containsKey(fid)) {
                  localToGlobal.put(localFeatNum, fid)
                  globalToLocal.put(fid, localFeatNum)
                  localFeatNum += 1
                }

                indices(i) = globalToLocal.get(fid)
              }
            case x: SparseDoubleSortedVector =>
              val len = x.size()
              val indices = x.getIndices

              for (i <- 0 until len) {
                val fid = indices(i)
                if (!globalToLocal.containsKey(fid)) {
                  localToGlobal.put(localFeatNum, fid)
                  globalToLocal.put(fid, localFeatNum)
                  localFeatNum += 1
                }
                indices(i) = globalToLocal.get(fid)
              }

            // If not SparseDummyVector or SparseDoubleSortedVector, throw exception
            case _ => throw new AngelException("data should be SparseDummyVector or SparseSortedDoubleVector")
          }
      }
    }

    // Convert a hashmap to an array for fast random access
    val localToGlobalArray = new Array[Int](localFeatNum)
    val iter = localToGlobal.int2IntEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      localToGlobalArray(entry.getIntKey) = entry.getIntValue
    }

    // Local model stored on workers
    val localModel = new LocalModel(new Array[Double](localFeatNum),
      new Array[Double](localFeatNum))

    // Index for convert local model to global model.
    val localIndex = new LocalIndex(localFeatNum, localToGlobalArray, globalToLocal)
    return (localModel, localIndex)
  }

  def split(data: DataBlock[LabeledData], splitNum: Int): Array[DataBlock[LabeledData]] = {
    data.resetReadIndex()
    data.shuffle()

    val total = data.size
    val numPerSplit = total / splitNum + 1
    val ret = new ArrayBuffer[DataBlock[LabeledData]]()
    var start = 0
    while (start < total) {
      ret.append(data.slice(start, Math.min(numPerSplit, total - start)))
      start += numPerSplit
      LOG.info(s"readerIdx=${ret.last.getReadIndex} writerIdx=${ret.last.getWriteIndex}")
    }

    ret.toArray
  }

  def updateUandX(z: T,
                  data: DataBlock[LabeledData],
                  localModel: LocalModel,
                  localIndex: LocalIndex,
                  rho: Double,
                  threadNum: Int): Unit = {
    // u_i = u_i + x_i - z_i
    for (i <- 0 until localIndex.localFeaNum)
      localModel.u(i) += localModel.x(i) - z.get((localIndex.localToGloabl(i)))

    // Minimize x

    val costFun = new CostFun(
      split(data, threadNum),
      new LogisticGradient(),
      new DenseDoubleVector(localIndex.localFeaNum, localModel.u),
      z,
      localIndex,
      rho)

    val lbfgs = new breeze.optimize.LBFGS[breeze.linalg.Vector[Double]](lbfgsMaxIter, numCorrection, lbfgsConvergenceTol)
    val summaries = lbfgs.iterations(
      new CachedDiffFunction[Vector[Double]](costFun),
      new DenseVector[Double](localModel.x))

    val lbfgsHistory = new ArrayBuffer[Double]
    var summary = summaries.next()
    while (summaries.hasNext) {
      lbfgsHistory += summary.value
      summary = summaries.next()
    }
    lbfgsHistory += summary.value
    localModel.x = summary.x.toArray
  }


  def updateW(model: SparseLRModel,
              localModel: LocalModel,
              localIndex: LocalIndex): Unit = {
    val update = new DenseDoubleVector(model.indexRange.toInt)
    update.setRowId(0)
    for (i <- 0 until localIndex.localFeaNum) {
      update.set(localIndex.localToGloabl(i), localModel.u(i) + localModel.x(i))
    }

    model.w.increment(update)
    model.w.clock().get()
  }


  def getT(z: T,
           model: SparseLRModel,
           localModel: LocalModel,
           localIndex: LocalIndex): Double = {
    val update = new DenseDoubleVector(1)

    var value = 0.0
    for (i <- 0 until localIndex.localFeaNum) {
      value += Math.pow(localModel.x(i) - z.get(localIndex.localToGloabl(i)), 2)
    }

    update.set(0, value)
    update.setRowId(0)

    model.t.increment(update)
    model.t.clock().get()
    return model.t.getRow(0).asInstanceOf[TDoubleVector].get(0)
  }

  def computeZ(z: T,
               model: SparseLRModel,
               regParam: Double,
               rho: Double,
               N: Int): Unit = {
    val w = model.w.getRow(0).asInstanceOf[TDoubleVector]
    val kappa = regParam / (rho * N)

    z.clear()

    for (i <- 0 until model.indexRange.toInt) {
      var value = w.get(i)
      value = value / N
      if (value > kappa)
        z.set(i, value - kappa)
      else if (value < -kappa)
        z.set(i, value + kappa)
    }
  }

  def calcLocalLoss(data: DataBlock[LabeledData],
                    localModel: LocalModel): Double = {
    val gradient = new LogisticGradient(2)
    var loss = 0.0
    data.resetReadIndex()
    val weights = new DenseDoubleVector(localModel.x.length, localModel.x)
    var finish = false
    while (!finish) {
      data.read() match {
        case null => finish = true
        case sample: LabeledData =>
          sample.getX match {
            case x: SparseDummyVector =>
              loss += gradient.compute(x, sample.getY, weights)
            case x: SparseDoubleSortedVector =>
              loss += gradient.compute(x, sample.getY, weights)
            case _ =>
              throw new AngelException("data should be SparseDummyVector or SparseDoubleSortedVector")
          }
      }
    }
    return loss
  }

  def getGlobalLoss(data: DataBlock[LabeledData],
                    model: SparseLRModel,
                    localModel: LocalModel,
                    z: T,
                    regParam: Double): Double = {

    val loss = calcLocalLoss(data, localModel)

    return loss
  }


  private class CostFun(
                         val data: Array[DataBlock[LabeledData]],
                         gradient: LogisticGradient,
                         u: DenseDoubleVector,
                         z: T,
                         localIndex: LocalIndex,
                         rho: Double,
                         var repeatTime: Int = 0) extends DiffFunction[breeze.linalg.Vector[Double]] {

    case class GradientAndLoss(var gradSum: DenseDoubleVector, var lossSum: Double, var countSum: Int) {
      def add(grad: DenseDoubleVector, loss: Double, count: Int): Unit = {
        this.synchronized {
          gradSum.plusBy(grad)
          lossSum += loss
          countSum += count
        }
      }
    }

    var gradientAndLoss: GradientAndLoss = _

    class CalThread(iter: DataBlock[LabeledData],
                    weight: DenseDoubleVector,
                    featNum: Int) extends Runnable {
      def run(): Unit = {
        iter.resetReadIndex()
        val grad = new DenseDoubleVector(featNum)
        var (loss, count) = (0.0, 0)
        var finish = false
        while (!finish) {
          iter.read() match {
            case sample: LabeledData =>
              sample.getX match {
                case point: SparseDummyVector =>
                  val l = gradient.compute(point, sample.getY, weight, grad)
                  loss += l
                  count += 1
                case point: SparseDoubleSortedVector =>
                  val l = gradient.compute(point, sample.getY, weight, grad)
                  loss += l
                  count += 1
                case _ => throw new AngelException("data should be SparseDummyVector or SparseSortedDoubleVector")
              }
            case null => finish = true
          }
        }
        gradientAndLoss.add(grad, loss, count)
      }
    }

    override def calculate(weights: breeze.linalg.Vector[Double]): (Double, breeze.linalg.Vector[Double]) = {
      repeatTime += 1
      val featNum = weights.length
      val mlWeights = new DenseDoubleVector(weights.length, weights.toArray)

      gradientAndLoss = GradientAndLoss(new DenseDoubleVector(featNum), 0.0, 0)
      val threadPool = new ThreadPoolExecutor(4, 8, 1, TimeUnit.HOURS,
        new LinkedBlockingQueue[Runnable])
      data.foreach(iter => threadPool.execute(new CalThread(iter, mlWeights, featNum)))
      threadPool.shutdown()

      // temp = x - z + u
      val temp = mlWeights.clone()
      temp.plusBy(u)
      val tv = temp.getValues
      for (i <- 0 until featNum)
        tv(i) -= z.get(localIndex.localToGloabl(i))

      val regLoss = 0.5 * rho * temp.dot(temp)

      temp.timesBy(rho)
      while (!threadPool.isTerminated) {
        Thread.sleep(1000)
        LOG.info(s"Await GradientCalculator Runnable to terminate")
      }

      temp.plusBy(gradientAndLoss.gradSum, 1.0 / gradientAndLoss.countSum)
      val loss = gradientAndLoss.lossSum / gradientAndLoss.countSum + regLoss
      LOG.info(s"repeat: $repeatTime loss: $loss(${gradientAndLoss.countSum}," +
        s"${gradientAndLoss.lossSum}, $regLoss), gradient nonZero: ${temp.nonZeroNumber()}")

      (loss, new DenseVector[Double](temp.getValues))
    }
  }


  private
  def validate(data: DataBlock[LabeledData],
               z: T,
               localIndex: LocalIndex,
               localModel: LocalModel): Double = {
    data.resetReadIndex()
    var finish = false
    var acn: Int = 0
    val total = data.size
    while (!finish) {
      data.read() match {
        case null => finish = true
        case sample: LabeledData =>
          sample.getX match {
            case x: SparseDummyVector =>
              val indices = x.getIndices
              var dot = 0.0
              for (i <- 0 until indices.length)
                dot += z.get(localIndex.localToGloabl(indices(i)))
              val score = Maths.sigmoid(dot)
              if (score >= 0.5 && sample.getY > 0)
                acn += 1
              if (score < 0.5 && sample.getY <= 0)
                acn += 1
            case x: SparseDoubleSortedVector =>
              val indices = x.getIndices
              val values = x.getValues
              var dot = 0.0
              for (i <- 0 until indices.length)
                dot += z.get(localIndex.localToGloabl(indices(i))) * values(i)
              val score = Maths.sigmoid(dot)
              if (score >= 0.5 && sample.getY > 0)
                acn += 1
              if (score < 0.5 && sample.getY <= 0)
                acn += 1
          }
      }
    }

    val precision = acn * 1.0 / total
    precision
  }


}
