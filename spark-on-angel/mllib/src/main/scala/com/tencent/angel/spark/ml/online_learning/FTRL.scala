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
import com.tencent.angel.psagent.matrix.MatrixClientFactory
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.ml.psf.ftrl.ComputeW
import com.tencent.angel.spark.ml.util.AutoPartitioner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import org.apache.spark.rdd.RDD

class FTRL(lambda1: Double, lambda2: Double, alpha: Double, beta: Double, regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: PSVector = _
  var nPS: PSVector = _
  var wPS: PSVector = _
  var name = "weights"


  /** Init with `dim` given, the default start index is 0
    * @param dim
    */
  def init(dim: Long): Unit =
    init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

  def init(dim: Long, rowType: RowType): Unit =
    init(dim, -1, rowType)

  def init(dim: Long, nnz: Long, rowType: RowType): Unit =
    init(dim, nnz, rowType, new ColumnRangePartitioner())

  /**
    * Init with dim, nnz, rowType and partitioner
    * @param dim, the index range is [0, dim) if dim > 0, else [long.min, long.max) is dim=-1
    * @param nnz, number-of-non-zero elements in model
    * @param rowType, default is T_FLOAT_SPARSE_LONGKEY
    * @param partitioner, default is column-range-partitioner
    */
  def init(dim: Long, nnz: Long, rowType: RowType, partitioner: Partitioner): Unit = {
    val ctx = new MatrixContext(name, 3, dim, nnz, -1, -1)
    ctx.setRowType(rowType)
    ctx.setPartitionerClass(partitioner.getClass)
    init(ctx)
  }

  /**
    * Init with start and end given
    * @param start, the start index
    * @param end, the end index range
    * @param nnz, the number of non-zero element in model
    * @param rowType, default is T_FLOAT_SPARSE_LONGKEY
    */
  def init(start: Long, end: Long, nnz: Long, rowType: RowType): Unit = {
    init(start, end, nnz, rowType, new ColumnRangePartitioner())
  }

  def init(start: Long, end: Long): Unit =
    init(start, end, -1, RowType.T_FLOAT_SPARSE_LONGKEY)

  def init(start: Long, end: Long, nnz: Long, rowType: RowType, partitioner: Partitioner): Unit = {
    val ctx = new MatrixContext(name, 3, start, end)
    ctx.setPartitionerClass(partitioner.getClass)
    ctx.setRowType(rowType)
    ctx.setValidIndexNum(nnz)
    init(ctx)
  }

  /**
    * Init model with the training data, this method will scan the index distribution in data
    * and automatically generate partitions with load balance into consideration
    * @param start, the start index
    * @param end, the end index
    * @param rowType, default is T_FLOAT_SPARSE_LONGKEY
    * @param data, training data
    * @param partitioner, a load balance partitioner
    */
  def init(start: Long, end: Long, rowType: RowType, data: RDD[Vector], partitioner: AutoPartitioner): Unit = {
    val ctx = new MatrixContext(name, 3, start, end)
    ctx.setRowType(rowType)
    partitioner.partition(data, ctx)
    init(ctx)
  }

  /**
    * create the model with a matrix-context and init three PSVector
    * @param ctx
    */
  def init(ctx: MatrixContext): Unit = {
    val matId = PSMatrixUtils.createPSMatrix(ctx)
    zPS = new PSVectorImpl(matId, 0, ctx.getColNum, ctx.getRowType)
    nPS = new PSVectorImpl(matId, 1, ctx.getColNum, ctx.getRowType)
    wPS = new PSVectorImpl(matId, 2, ctx.getColNum, ctx.getRowType)
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

    PSContext.instance()
    val client = MatrixClientFactory.get(zPS.poolId, PSContext.getTaskId)
    val vectors = client.get(Array(0, 1), indices)
    val (localZ, localN) = (vectors(0), vectors(1))


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

      deltaZ.iadd(grad)
      Ufuncs.iaxpy2(deltaN, grad, 1)
      val delta = OptFuncs.ftrldelta(grad, localN, alpha)
      assert(delta.getSize == grad.getSize)
      deltaZ.isub(delta.imul(weight))

//      val featureIndices = feature match {
//        case longKey: LongKeyVector => longKey.getStorage
//          .asInstanceOf[LongKeyVectorStorage].getIndices
//        case dummyV: LongDummyVector => dummyV.getIndices
//      }
//      val indicesValue = featureIndices.map{ _ =>1.0f}
//      val featureN = VFactory.sortedLongKeyFloatVector(dim, featureIndices, indicesValue).imul(localN)
//      val delta = OptFuncs.ftrldelta(featureN, grad, alpha)

      println(s"margin=${margin} multiplier=${multiplier}")

      val loss = if (label > 0) log1pExp(margin) else log1pExp(margin) - margin

      lossSum += loss
//      Ufuncs.iaxpy2(deltaN, grad, 1)
//      deltaZ.iadd(grad.isub(delta.imul(weight)))
    }
    end = System.currentTimeMillis()
    val optimTime = end - start

    start = System.currentTimeMillis()
    val update = new Array[Vector](2)
    update(0) = deltaZ
    update(1) = deltaN
    client.increment(Array(0, 1), update, true)
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
    * calculate w from z and n and store it in the w row
    *
    * @return
    */
  def weight: PSVector = {
    val func = new ComputeW(wPS.poolId, alpha, beta, lambda1, lambda2)
    wPS.psfUpdate(func).get()
    wPS
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }

  /**
    * Save z and n for increment training
    * @param path
    */
  def save(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelSaveContext(path)
    val matrixContext = new MatrixSaveContext(name, format)
    matrixContext.addIndices(Array(0, 1))
    modelContext.addMatrix(matrixContext)
    AngelPSContext.save(modelContext)
  }

  /**
    * Save w for model serving
    * @param path
    */
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