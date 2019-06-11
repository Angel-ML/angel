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

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{LongDummyVector, LongKeyVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.{ColIdValueTextRowFormat, RowIdColIdValueTextRowFormat}
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.partitioner.{ColumnRangePartitioner, Partitioner}
import com.tencent.angel.spark.context.AngelPSContext
import com.tencent.angel.spark.ml.psf.ftrl.ComputeW
import com.tencent.angel.spark.ml.util.AutoPartitioner
import com.tencent.angel.spark.models.impl.{PSMatrixImpl, PSVectorImpl}
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import org.apache.spark.rdd.RDD

class FTRL() extends Serializable {

  var lambda1: Double = 0
  var lambda2: Double = 0
  var alpha: Double = 0
  var beta: Double = 0
  var wPS: PSVector = _
  var name = "weights"
  var possionRate: Float = 1.0f
  var matrix: PSMatrix = _

  def this(lambda1: Double, lambda2: Double, alpha: Double, beta: Double) {
    this()
    this.lambda1 = lambda1
    this.lambda2 = lambda2
    this.alpha = alpha
    this.beta = beta
  }

  /** Init with `dim` given, the default start index is 0
    *
    * @param dim , [0, dim)
    */
  def init(dim: Long): Unit =
    init(dim, RowType.T_FLOAT_SPARSE_LONGKEY)

  def init(dim: Long, rowType: RowType): Unit =
    init(dim, -1, rowType)

  def init(dim: Long, nnz: Long, rowType: RowType): Unit =
    init(dim, nnz, rowType, new ColumnRangePartitioner())

  /**
    * Init with dim, nnz, rowType and partitioner
    *
    * @param dim         , the index range is [0, dim) if dim > 0, else [long.min, long.max) is dim=-1
    * @param nnz         , number-of-non-zero elements in model
    * @param rowType     , default is T_FLOAT_SPARSE_LONGKEY
    * @param partitioner , default is column-range-partitioner
    */
  def init(dim: Long, nnz: Long, rowType: RowType, partitioner: Partitioner): Unit = {
    val ctx = new MatrixContext(name, 3, dim, nnz, -1, -1)
    ctx.setRowType(rowType)
    ctx.setPartitionerClass(partitioner.getClass)
    init(ctx)
  }

  /**
    * create the model with a matrix-context and init three PSVector
    *
    * @param ctx , matrix context
    */
  def init(ctx: MatrixContext): Unit = {
    val matId = PSMatrixUtils.createPSMatrix(ctx)
    wPS = new PSVectorImpl(matId, 2, ctx.getColNum, ctx.getRowType)
    matrix = new PSMatrixImpl(matId, ctx.getRowNum, ctx.getColNum, ctx.getRowType)
  }

  def init(start: Long, end: Long): Unit =
    init(start, end, -1, RowType.T_FLOAT_SPARSE_LONGKEY)

  /**
    * Init with start and end given
    *
    * @param start   , the start index
    * @param end     , the end index range
    * @param nnz     , the number of non-zero element in model
    * @param rowType , default is T_FLOAT_SPARSE_LONGKEY
    */
  def init(start: Long, end: Long, nnz: Long, rowType: RowType): Unit = {
    init(start, end, nnz, rowType, new ColumnRangePartitioner())
  }

  def init(start: Long, end: Long, nnz: Long, rowType: RowType,
           partitioner: Partitioner): Unit = {
    val ctx = new MatrixContext(name, 3, start, end)
    ctx.setPartitionerClass(partitioner.getClass)
    ctx.setRowType(rowType)
    ctx.setValidIndexNum(nnz)
    init(ctx)
  }

  /**
    * Init model with the training data, this method will scan the index distribution in data
    * and automatically generate partitions with load balance into consideration
    *
    * @param start       , the start index
    * @param end         , the end index
    * @param rowType     , default is T_FLOAT_SPARSE_LONGKEY
    * @param data        , training data
    * @param partitioner , a load balance partitioner
    */
  def init(start: Long, end: Long, rowType: RowType, data: RDD[Vector],
           partitioner: AutoPartitioner): Unit = {
    val ctx = new MatrixContext(name, 3, start, end)
    ctx.setRowType(rowType)
    partitioner.partition(data, ctx)
    init(ctx)
  }

  def setPossionRate(possionRate: Float): Unit =
    this.possionRate = possionRate


  /**
    * Optimize a batch of data with FTRL optimizer
    *
    * @param batch , data batch
    * @return summation of loss for this batch
    */
  def optimize(batch: Array[LabeledData]): Double = {
    var (start, end) = (0L, 0L)

    // First, distinct the feature indices of this batch
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    start = System.currentTimeMillis()

    // Fetch the dimensions of n/z

    val vectors = matrix.pull(Array(0, 1), indices)
    val (localZ, localN) = (vectors(0), vectors(1))

    assert(localN.getSize == indices.length)
    assert(localZ.getSize == indices.length)

    end = System.currentTimeMillis()
    val pullTime = end - start


    start = System.currentTimeMillis()

    // calculate w with FTRL
    val weight = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)
    val dim = batch.head.getX.dim()

    // allocate two vectors for delta n/z
    // TODO: use emptylike instead
    val deltaZ = localZ.copy()
    val deltaN = localN.copy()
    deltaZ.clear()
    deltaN.clear()

    val iter = batch.iterator
    var lossSum = 0.0

    while (iter.hasNext) {
      val point = iter.next()
      val (feature, label) = (point.getX, point.getY)
      // calculate margin
      val margin = -weight.dot(feature)
      val multiplier = 1.0 / (1.0 + math.exp(margin)) - label

      // sample the feature index with Possion sampling
      val possion = Ufuncs.ftrlpossion(localN, feature, possionRate).ifilter(10e-10)
      val grad = possion.imul(multiplier)

      // calculate delta z/n
      deltaZ.iadd(grad)
      Ufuncs.iaxpy2(deltaN, grad, 1)
      OptFuncs.iftrldetalintersect(grad, localN, alpha)
      deltaZ.isub(grad.imul(weight))

      val loss = if (label > 0) log1pExp(margin) else log1pExp(margin) - margin
      lossSum += loss
    }
    end = System.currentTimeMillis()
    val optimTime = end - start

    // push delta z/n
    start = System.currentTimeMillis()
    matrix.increment(Array(0, 1), Array(deltaZ, deltaN))
    end = System.currentTimeMillis()
    val pushTime = end - start

    println(s"${lossSum / batch.size} " +
      s"pullTime=$pullTime " +
      s"optimTime=$optimTime " +
      s"pushTime=$pushTime")
    lossSum
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }

  /**
    * Predict with weight
    *
    * @param batch
    * @return
    */
  def predict(batch: Array[LabeledData], isTraining: Boolean = true): Array[(Double, Double)] = {
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    val weight = isTraining match {
      case true =>
        val vectors = matrix.pull(Array(0, 1), indices)
        val (localZ, localN) = (vectors(0), vectors(1))
        Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)
      case false =>
        matrix.pull(Array(2), indices)(0)
    }
    // Fetch the dimensions of n/z

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
    val func = new ComputeW(matrix.id, alpha, beta, lambda1, lambda2, 1.0)
    wPS.psfUpdate(func).get()
    wPS
  }

  /**
    * Save z and n for increment training
    *
    * @param path , output path
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
    *
    * @param path , output path
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