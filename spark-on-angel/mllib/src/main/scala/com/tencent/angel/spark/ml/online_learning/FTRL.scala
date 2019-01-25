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

import java.util

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, LongDummyVector, LongFloatVector, LongKeyVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.{ColIdValueTextRowFormat, RowIdColIdValueTextRowFormat}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.partitioner.{ColumnRangePartitioner, Partitioner}
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.psf.ftrl.{ComputeW, FTRLPartitioner}
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl

class FTRL(lambda1: Double, lambda2: Double, alpha: Double, beta: Double, regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: PSVector = _
  var nPS: PSVector = _
  var wPS: PSVector = _
  var name = "weights"

  def init(dim: Long, rowType: RowType): Unit = {
    init(dim, -1, rowType)
  }

  def init(dim: Long, nnz: Long, rowType: RowType, partitioner: Partitioner): Unit = {
    zPS = PSVector.longKeySparse(dim, nnz, 3, rowType,
      additionalConfiguration = Map(AngelConf.Angel_PS_PARTITION_CLASS -> partitioner.getClass.getName))
    nPS = PSVector.duplicate(zPS)
    wPS = PSVector.duplicate(zPS)
    name = PSContext.instance().getMatrixMeta(zPS.poolId).get.getName
  }

  def init(dim: Long, nnz: Long, rowType: RowType): Unit = {
    init(dim, nnz, rowType, new ColumnRangePartitioner())
  }

  def init(dim: Long): Unit = init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

  def init(start: Long, end: Long, nnz: Long, rowType: RowType): Unit = {
    val ctx = new MatrixContext()
    ctx.setName(name)
    ctx.setColNum(end)
    ctx.setRowNum(3)
    ctx.setPartitionerClass(classOf[FTRLPartitioner])
    ctx.setRowType(rowType)
    ctx.setValidIndexNum(nnz)
    ctx.setMaxColNumInBlock(start)
    val matId = PSMatrixUtils.createPSMatrix(ctx)

    zPS = new PSVectorImpl(matId, 0, end, rowType)
    nPS = new PSVectorImpl(matId, 1, end, rowType)
    wPS = new PSVectorImpl(matId, 2, end, rowType)
  }


  def optimize(batch: Array[LabeledData]): Double = {

    var (start, end) = (0L, 0L)
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    start = System.currentTimeMillis()
    val localZ = zPS.pull(indices)
    val localN = nPS.pull(indices)
    end = System.currentTimeMillis()
    val pullTime = end - start


    start = System.currentTimeMillis()
    val weight = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)
    val dim = batch.head.getX.dim()
    val deltaZ = VFactory.sparseLongKeyFloatVector(dim)
    val deltaN = VFactory.sparseLongKeyFloatVector(dim)

    val iter = batch.iterator
    var lossSum = 0.0
    while (iter.hasNext) {
      val point = iter.next()
      val (feature, label) = (point.getX, point.getY)
      val margin = -weight.dot(feature)
      val multiplier = 1.0 / (1.0 + math.exp(margin)) - label
      val grad = feature match {
        case x: LongDummyVector =>
          val values = new Array[Float](x.getIndices.length)
          util.Arrays.fill(values, multiplier.toFloat)
          VFactory.sparseLongKeyFloatVector(dim, x.getIndices, values)
        case _ =>
          feature.mul(multiplier)
      }

      val featureIndices = feature match {
        case longV: LongDoubleVector => longV.getStorage.getIndices
        case longV: LongFloatVector => longV.getStorage.getIndices
        case dummyV: LongDummyVector => dummyV.getIndices
      }
      val indicesValue = featureIndices.map{ _ =>1.0f}
      val featureN = VFactory.sparseLongKeyFloatVector(dim, featureIndices, indicesValue).mul(localN)
      val delta = OptFuncs.ftrldelta(featureN, grad, alpha)

      val loss = if (label > 0) log1pExp(margin) else log1pExp(margin) - margin

      lossSum += loss
      Ufuncs.iaxpy2(deltaN, grad, 1)
      deltaZ.iadd(grad.isub(delta.imul(weight)))
    }
    end = System.currentTimeMillis()
    val optimTime = end - start

    start = System.currentTimeMillis()
    zPS.increment(deltaZ)
    nPS.increment(deltaN)
    end = System.currentTimeMillis()
    val pushTime = end - start

    println(s"${lossSum / batch.size} pullTime=$pullTime optimTime=$optimTime pushTime=$pushTime")

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
//    val wPS = PSVector.duplicate(zPS)
    val func = new ComputeW(wPS.poolId, alpha, beta, lambda1, lambda2)
    wPS.psfUpdate(func).get()
    wPS
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
    val matrixContext = new MatrixSaveContext(name, format)
    matrixContext.addIndices(Array(0, 1, 2))
    modelContext.addMatrix(matrixContext)
    AngelPSContext.save(modelContext)
  }

  def saveWeight(path: String): Unit = {
    val format = classOf[ColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelSaveContext(path)
    val matrixContext = new MatrixSaveContext(name, format)
    matrixContext.addIndices(Array(2))
    modelContext.addMatrix(matrixContext)
    AngelPSContext.save(modelContext)
  }

  def load(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelLoadContext(path)
    val matrixContext = new MatrixLoadContext(name, format)
    modelContext.addMatrix(matrixContext)
    AngelPSContext.load(modelContext)
  }
}