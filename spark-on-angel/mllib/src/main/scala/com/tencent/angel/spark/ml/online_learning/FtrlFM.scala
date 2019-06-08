package com.tencent.angel.spark.ml.online_learning

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.storage.LongKeyVectorStorage
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{LongDummyVector, LongKeyVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.RowIdColIdValueTextRowFormat
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.storage.partitioner.{ColumnRangePartitioner, Partitioner}
import com.tencent.angel.spark.context.AngelPSContext
import com.tencent.angel.spark.ml.psf.ftrl.ComputeW
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl

class FtrlFM() extends Serializable {

  val firstName = "first"
  val secondName = "second"
  var lambda1: Double = 0
  var lambda2: Double = 0
  var alpha: Double = 0
  var beta: Double = 0
  var first: PSMatrix = _
  var second: PSMatrix = _
  var factor: Int = 0

  def this(lambda1: Double, lambda2: Double, alpha: Double, beta: Double) {
    this()
    this.lambda1 = lambda1
    this.lambda2 = lambda2
    this.alpha = alpha
    this.beta = beta
  }

  def init(dim: Long, factor: Int): Unit = {
    init(dim, RowType.T_FLOAT_SPARSE_LONGKEY, factor)
  }

  def init(dim: Long, rowType: RowType, factor: Int): Unit = {
    init(dim, -1, rowType, factor)
  }

  def init(dim: Long, nnz: Long, rowType: RowType, factor: Int): Unit = {
    init(dim, nnz, rowType, factor, new ColumnRangePartitioner())
  }

  /**
    * Init with dim, nnz, rowType, factor and partitioner
    *
    * @param dim         , the index range is [0, dim) if dim>0, else [long.min, long.max) if dim=-1 and rowType is sparse
    * @param nnz         , number-of-non-zero elements in model
    * @param rowType     , default is T_FLOAT_SPARSE_LONGKEY
    * @param factor      , num of factors
    * @param partitioner , default is column-range-partitioner
    */
  def init(dim: Long, nnz: Long, rowType: RowType, factor: Int, partitioner: Partitioner): Unit = {
    val firstCtx = new MatrixContext(firstName, 3, dim,
      nnz, -1, -1)
    firstCtx.setRowType(rowType)
    firstCtx.setPartitionerClass(partitioner.getClass)
    first = init(firstCtx)

    val secondCtx = new MatrixContext(secondName, factor * 3, dim,
      nnz, -1, -1)
    secondCtx.setRowType(rowType)
    secondCtx.setPartitionerClass(partitioner.getClass)
    second = init(secondCtx)
    this.factor = factor
  }

  /**
    * create the model with a matrix-context and init three PSVector
    *
    * @param ctx , matrix context
    */
  def init(ctx: MatrixContext): PSMatrix = {
    val matId = PSMatrixUtils.createPSMatrix(ctx)
    new PSMatrixImpl(matId, ctx.getRowNum, ctx.getColNum, ctx.getRowType)
  }

  def init(start: Long, end: Long, factor: Int): Unit = {
    init(start, end, -1, RowType.T_FLOAT_SPARSE_LONGKEY, factor)
  }

  def init(start: Long, end: Long, nnz: Long, rowType: RowType, factor: Int): Unit = {
    init(start, end, nnz, rowType, factor, new ColumnRangePartitioner())
  }

  def init(start: Long, end: Long, nnz: Long, rowType: RowType,
           factor: Int,
           partitioner: Partitioner): Unit = {
    val firstCtx = new MatrixContext(firstName, 3, start, end)
    firstCtx.setPartitionerClass(partitioner.getClass)
    firstCtx.setRowType(rowType)
    firstCtx.setValidIndexNum(nnz)
    first = init(firstCtx)

    val secondCtx = new MatrixContext(secondName, factor * 3, start, end)
    secondCtx.setPartitionerClass(partitioner.getClass)
    secondCtx.setRowType(rowType)
    secondCtx.setValidIndexNum(nnz)
    second = init(secondCtx)

    this.factor = factor
  }

  def optimize(index: Int, batch: Array[LabeledData]): Double = {

    var (start, end) = (0L, 0L)

    start = System.currentTimeMillis()
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    // fetch first
    val firsts = first.pull(Array(0, 1), indices)
    val (localZ, localN) = (firsts(0), firsts(1))
    val localW = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)

    // fetch second
    val seconds = second.pull((0 until factor * 2).toArray, indices)
    val localV = (0 until factor).map(idx => Ufuncs.ftrlthresholdinit(seconds(idx), seconds(idx + factor),
      alpha, beta, lambda1, lambda2, 0.0, 0.01)).toArray

    val localV2 = localV.map(v0 => v0.mul(v0))

    end = System.currentTimeMillis()
    val pullTime = end - start


    val deltaZ = localZ.emptyLike()
    val deltaN = localN.emptyLike()

    val deltaV = seconds.map(f => f.emptyLike())

    start = System.currentTimeMillis()

    val iter = batch.iterator
    var lossSum = 0.0
    while (iter.hasNext) {
      val point = iter.next()
      val (feature, label) = (point.getX, point.getY)
      val (gradW, gradV, loss) = gradient(localW, localV, localV2, label, feature)

      delta(gradW, localN, localW, deltaZ, deltaN)
      (0 until factor).foreach { idx =>
        delta(gradV(idx), seconds(idx + factor), localV(idx),
          deltaV(idx), deltaV(idx + factor))
      }

      lossSum += loss
    }
    end = System.currentTimeMillis()
    val optimTime = end - start

    start = System.currentTimeMillis()
    first.increment(Array(0, 1), Array(deltaZ, deltaN))
    second.increment((0 until factor * 2).toArray, deltaV)
    val pushTime = System.currentTimeMillis() - start

    println(s"batchId=$index loss=${lossSum / batch.size} pullTime=$pullTime optimTime=$optimTime pushTime=$pushTime")
    lossSum
  }

  def delta(grad: Vector, localN: Vector, weight: Vector,
            deltaZ: Vector, deltaN: Vector): Unit = {
    deltaZ.iadd(grad)
    Ufuncs.iaxpy2(deltaN, grad, 1)
    OptFuncs.iftrldetalintersect(grad, localN, alpha)
    deltaZ.isub(grad.imul(weight))
  }

  def gradient(w: Vector, v: Array[Vector], v2: Array[Vector],
               label: Double, feature: Vector): (Vector, Array[Vector], Double) = {


    val marginW = w.dot(feature)
    val vdot = v.map(v0 => v0.dot(feature))
    val f2 = feature.mul(feature)
    val marginV = v.zip(vdot).zip(v2).map { case ((v0, dot), v2) =>
      dot * dot - v2.dot(f2)
    }

    val margin = -(marginW + marginV.sum / 2.0)
    val multiplier = 1.0 / (1.0 + math.exp(margin)) - label
    val gradW = feature.mul(multiplier)


    val gradV = v.zip(vdot).map { case (v0, dot) =>
      val grad = Ufuncs.fmgrad(feature, v0, dot)
      grad.imul(multiplier)
      grad
    }

    val loss = if (label > 0) log1pExp(margin) else log1pExp(margin) - margin
    (gradW, gradV, loss)
  }

  def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }

  def weight(): Unit = {
    val func1 = new ComputeW(first.id, alpha, beta, lambda1, lambda2, 1.0)
    first.psfUpdate(func1).get()

    val func2 = new ComputeW(second.id, alpha, beta, lambda1, lambda2, factor)
    second.psfUpdate(func2).get()
  }

  def predict(batch: Array[LabeledData], isTraining: Boolean = true): Array[(Double, Double)] = {
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case dummy: LongDummyVector => dummy.getIndices
          case longKey: LongKeyVector => longKey.getStorage
            .asInstanceOf[LongKeyVectorStorage].getIndices
        }
    }.distinct

    val (localW: Vector, localV: Array[Vector]) = isTraining match {
      case true =>
        // fetch first
        val firsts = first.pull(Array(0, 1), indices)
        val (localZ, localN) = (firsts(0), firsts(1))
        val localW = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)

        // fetch second
        val seconds = second.pull((0 until factor * 2).toArray, indices)
        val localV = (0 until factor).map(idx => Ufuncs.ftrlthresholdinit(seconds(idx), seconds(idx + factor),
          alpha, beta, lambda1, lambda2, 0.0, 0.01)).toArray
        (localW, localV)

      case false =>
        val localW = first.pull(Array(2), indices)(0)
        val localV = second.pull((factor * 2 until factor * 3).toArray, indices)
        (localW, localV)
    }

    val localV2 = localV.map(v => v.mul(v))

    batch.map { point =>
      val (feature, label) = (point.getX, point.getY)
      val p = predict(localW, localV, localV2, feature)
      val score = 1 / (1 + math.exp(-p))
      (label, score)
    }
  }

  def predict(w: Vector, v: Array[Vector], v2: Array[Vector], feature: Vector): Double = {
    val sumW = w.dot(feature)
    val f2 = feature.mul(feature)
    val sumV = v.zip(v2).map { case (v0, v2) =>
      val t1 = v0.dot(feature)
      t1 * t1 - v2.dot(f2)
    }
    return sumW + sumV.sum / 2.0
  }

  def save(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext1 = new ModelSaveContext(path + "/weightW")
    val matrixContext1 = new MatrixSaveContext(firstName, format)
    matrixContext1.addIndices(Array(0, 1))
    modelContext1.addMatrix(matrixContext1)
    AngelPSContext.save(modelContext1)


    val modelContext2 = new ModelSaveContext(path + "/weightV")
    val matrixContext2 = new MatrixSaveContext(secondName, format)
    matrixContext2.addIndices((0 until factor * 2).toArray)
    modelContext2.addMatrix(matrixContext2)
    AngelPSContext.save(modelContext2)
  }

  /**
    * Save w, v for model serving
    *
    * @param path , output path
    */
  def saveWeight(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext1 = new ModelSaveContext(path + "/weightW")
    val matrixContext1 = new MatrixSaveContext(firstName, format)
    matrixContext1.addIndices(Array(2))
    modelContext1.addMatrix(matrixContext1)
    AngelPSContext.save(modelContext1)

    val modelContext2 = new ModelSaveContext(path + "/weightV")
    val matrixContext2 = new MatrixSaveContext(secondName, format)
    matrixContext2.addIndices((factor * 2 until factor * 3).toArray)
    modelContext2.addMatrix(matrixContext2)
    AngelPSContext.save(modelContext2)
  }

  def load(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext1 = new ModelLoadContext(path + "/weightW")
    val matrixContext1 = new MatrixLoadContext(firstName, format)
    modelContext1.addMatrix(matrixContext1)
    AngelPSContext.load(modelContext1)

    val modelContext2 = new ModelLoadContext(path + "/weightV")
    val matrixContext2 = new MatrixLoadContext(secondName, format)
    modelContext2.addMatrix(matrixContext2)
    AngelPSContext.load(modelContext2)
  }
}
