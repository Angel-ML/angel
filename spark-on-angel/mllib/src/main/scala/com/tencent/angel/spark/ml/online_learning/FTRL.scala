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


package com.tencent.angel.spark.ml.online_learning

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, LongDummyVector, LongKeyVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.model.output.format.RowIdColIdValueTextRowFormat
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.psf.FTRLWUpdater
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.util.VectorUtils

class FTRL(lambda1: Double, lambda2: Double, alpha: Double, beta: Double, regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: PSVector = _
  var nPS: PSVector = _

  def init(dim: Long, rowType: RowType): Unit = {
    zPS = PSVector.longKeySparse(dim, -1, 3, rowType,
      additionalConfiguration = Map(AngelConf.Angel_PS_PARTITION_CLASS -> classOf[ColumnRangePartitioner].getName))
    nPS = PSVector.duplicate(zPS)
  }

  def init(dim: Long): Unit = init(dim, RowType.T_DOUBLE_SPARSE_LONGKEY)

  /**
    * Using math2 to optimize FTRL, but there is some error for this version.
    * The loss will become infinity when reaching convergence.
    *
    * @param batch : mini-batch training examples
    * @return
    */
  def optimize(batch: Array[LabeledData]): Double = {
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    val localZ = zPS.pull(indices)
    val localN = nPS.pull(indices)
    val weight = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)

    val deltaZ = localZ.copy()
    val deltaN = localN.copy()
    deltaZ.clear()
    deltaN.clear()

    val iter = batch.iterator
    var lossSum = 0.0
    while (iter.hasNext) {
      val point = iter.next()
      val (feature, label) = (point.getX, point.getY)
      val margin = -weight.dot(feature)
      val multiplier = 1.0 / (1.0 + math.exp(margin)) - label
      val grad = feature.mul(multiplier)
      val delta = OptFuncs.ftrldelta(localN, grad, alpha)

      val loss = if (label > 0) log1pExp(margin) else log1pExp(margin) - margin

      lossSum += loss
      Ufuncs.iaxpy2(deltaN, grad, 1)
      deltaZ.iadd(grad.isub(delta.imul(weight)))
    }

    deltaZ.idiv(batch.length)
    deltaN.idiv(batch.length)
    zPS.increment(deltaZ)
    nPS.increment(deltaN)

    println(s"${lossSum / batch.size}")

    lossSum
  }

  /**
    * Predict with weight
    *
    * @param batch
    * @return
    */
  def predict(batch: Array[LabeledData]): Array[(Double, Double)] = {
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    val localZ = zPS.pull(indices)
    val localN = nPS.pull(indices)
    val weight = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)

    batch.map {
      case point =>
        val (feature, label) = (point.getX, point.getY)
        val p = weight.dot(feature)
        val score = 1 / (1 + math.exp(-p))
        (label, score)
    }
  }

  /**
    * Optimizing only for LongDoubleVector. This version is ok and the model is correct.
    *
    * @param batch   : training mini-batch examples
    * @param costFun : function to calculate gradients
    * @return Loss for this batch
    */
  def optimize(batch: Array[(Vector, Double)],
               costFun: (LongDoubleVector, Double, Vector) => (LongDoubleVector, Double)): Double = {

    val dim = batch.head._1.dim()
    val featIds = batch.flatMap { case (v, _) =>
      v match {
        case longV: LongDoubleVector => longV.getStorage.getIndices
        case dummyV: LongDummyVector => dummyV.getIndices
        case _ => throw new Exception("only support SparseVector and DummyVector")
      }
    }.distinct

    val localZ = zPS.pull(featIds).asInstanceOf[LongDoubleVector]
    val localN = nPS.pull(featIds).asInstanceOf[LongDoubleVector]

    val deltaZ = VFactory.sparseLongKeyDoubleVector(dim)
    val deltaN = VFactory.sparseLongKeyDoubleVector(dim)

    val fetaValues = featIds.map { fId =>
      val zVal = localZ.get(fId)
      val nVal = localN.get(fId)

      updateWeight(fId, zVal, nVal, alpha, beta, lambda1, lambda2)
    }

    val localW = VFactory.sparseLongKeyDoubleVector(dim, featIds, fetaValues)

    val lossSum = batch.map { case (feature, label) =>
      optimize(feature, label, localN, localW, deltaZ, deltaN, costFun)
    }.sum

    zPS.increment(deltaZ)
    nPS.increment(deltaN)

    println(s"${lossSum / batch.length}")

    lossSum
  }

  /**
    * Optimizing for one example (feature, label)
    *
    * @param feature
    * @param label
    * @param localN , N in the local executor
    * @param localW . weight in the local executor
    * @param deltaZ , delta value for z
    * @param deltaN , delta value for n
    * @param costFun
    * @return
    */
  def optimize(feature: Vector,
               label: Double,
               localN: LongDoubleVector,
               localW: LongDoubleVector,
               deltaZ: LongDoubleVector,
               deltaN: LongDoubleVector,
               costFun: (LongDoubleVector, Double, Vector) => (LongDoubleVector, Double)
              ): Double = {

    val featIndices = feature match {
      case longV: LongDoubleVector => longV.getStorage.getIndices
      case dummyV: LongDummyVector => dummyV.getIndices
    }

    val (newGradient, loss) = costFun(localW, label, feature)

    featIndices.foreach { fId =>
      val nVal = localN.get(fId)
      val gOnId = newGradient.get(fId)
      val dOnId = 1.0 / alpha * (Math.sqrt(nVal + gOnId * gOnId) - Math.sqrt(nVal))

      deltaZ.set(fId, deltaZ.get(fId) + gOnId - dOnId * localW.get(fId))
      deltaN.set(fId, deltaN.get(fId) + gOnId * gOnId)
    }
    (loss)
  }

  /**
    * calculate w from z and n and store it in the w row
    *
    * @return
    */
  def weight: PSVector = {
    val wPS = PSVector.duplicate(zPS)
    val func = new FTRLWUpdater(alpha, beta, lambda1, lambda2, regularSkipFeatIndex)
    VectorUtils.zip2MapWithIndex(zPS, nPS, func, wPS)
    VectorUtils.compress(wPS)
  }

  /**
    * calculate w from z and n for one dimension
    *
    * @param fId
    * @param zOnId
    * @param nOnId
    * @param alpha
    * @param beta
    * @param lambda1
    * @param lambda2
    * @return
    */
  def updateWeight(fId: Long,
                   zOnId: Double,
                   nOnId: Double,
                   alpha: Double,
                   beta: Double,
                   lambda1: Double,
                   lambda2: Double): Double = {
    if (fId == regularSkipFeatIndex) {
      -1.0 * alpha * zOnId / (beta + Math.sqrt(nOnId))
    } else if (Math.abs(zOnId) <= lambda1) {
      0.0
    } else {
      (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nOnId)) / alpha)) * (zOnId - Math.signum(zOnId).toInt * lambda1)
    }
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }

  def save(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelSaveContext(path)
    val name = PSContext.instance().getMatrixMeta(zPS.poolId).get.getName
    val matrixContext = new MatrixSaveContext(name, format)
    matrixContext.addIndices(Array(0, 1, 2))
    modelContext.addMatrix(matrixContext)
    AngelPSContext.save(modelContext)
  }

  def load(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelLoadContext(path)
    val name = PSContext.instance().getMatrixMeta(zPS.poolId).get.getName
    val matrixContext = new MatrixLoadContext(name, format)
    modelContext.addMatrix(matrixContext)
    AngelPSContext.load(modelContext)
  }
}