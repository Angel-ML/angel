package com.tencent.angel.spark.ml.online_learning

import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.storage.{IntKeyVectorStorage, LongKeyVectorStorage}
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import com.tencent.angel.ml.math2.vector.{IntFloatVector, IntKeyVector, LongKeyVector, Vector}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.{ColIdValueTextRowFormat, RowIdColIdValueTextRowFormat}
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.ps.storage.partitioner.Partitioner
import com.tencent.angel.spark.context.AngelPSContext
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl

class FtrlFM(lambda1: Double, lambda2: Double, alpha: Double, beta: Double) extends Serializable {

  var first: PSMatrix = _
  val firstName = "first"
  var second: PSMatrix = _
  val secondName = "second"

  var dim: Int = 0

  def init(start: Long, end: Long, nnz: Long, rowType: RowType,
           dim: Int,
           partitioner: Partitioner): Unit = {
    val firstCtx = new MatrixContext(firstName, 3, start, end)
    firstCtx.setPartitionerClass(partitioner.getClass)
    firstCtx.setRowType(rowType)
    firstCtx.setValidIndexNum(nnz)
    first = init(firstCtx)

    val secondCtx = new MatrixContext(secondName, dim * 3, start, end)
    secondCtx.setPartitionerClass(partitioner.getClass)
    secondCtx.setRowType(rowType)
    secondCtx.setValidIndexNum(nnz)
    second = init(secondCtx)

    this.dim = dim
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

    def random(): Unit = {
      val func1 = new RandomNormal(first.id, 0, 0, 0.01)
      first.psfUpdate(func1).get()

      val func2 = new RandomNormal(second.id, 0, dim, 0, 0.01)
      second.psfUpdate(func2).get()
    }

  def optimize(index: Int, batch: Array[LabeledData]): Double = {

    var (start, end) = (0L, 0L)

    start = System.currentTimeMillis()
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case intKey: IntKeyVector => intKey.getStorage
            .asInstanceOf[IntKeyVectorStorage].getIndices
        }
    }.distinct

    // fetch first
    val firsts = first.pull(Array(0, 1), indices)
    val (localZ, localN) = (firsts(0), firsts(1))
    val localW = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)

    // fetch second
    val seconds = second.pull((0 until dim*2).toArray, indices)
    val localV = (0 until dim).map(idx => Ufuncs.ftrlthresholdinit(seconds(idx), seconds(idx+dim),
      alpha, beta, lambda1, lambda2, 0.0, 0.01)).toArray

    val localV2 = localV.map(v0 => v0.mul(v0))

    end = System.currentTimeMillis()
    val pullTime = end - start



    val deltaZ = localZ.copy()
    val deltaN = localN.copy()
    deltaN.clear()
    deltaZ.clear()

    val deltaV = seconds.map(f => f.copy())
    deltaV.foreach(f => f.clear())

    start = System.currentTimeMillis()

    val iter = batch.iterator
    var lossSum = 0.0
    while (iter.hasNext) {
      val point = iter.next()
      val (feature, label) = (point.getX, point.getY)
      val (gradW, gradV, loss) = gradient(localW, localV, localV2, label, feature)

      delta(gradW, localN, localW, deltaZ, deltaN)
      (0 until dim).foreach { idx =>
        delta(gradV(idx), seconds(idx+dim), localV(idx),
          deltaV(idx), deltaV(idx+dim))
      }

      lossSum += loss
    }
    end = System.currentTimeMillis()
    val optimTime = end - start

    start = System.currentTimeMillis()
    first.increment(Array(0, 1), Array(deltaZ, deltaN))
    second.increment((0 until dim*2).toArray, deltaV)
    val pushTime = System.currentTimeMillis() - start

    println(s"batchId=$index loss=${lossSum/batch.size} pullTime=$pullTime optimTime=$optimTime pushTime=$pushTime")
    lossSum
  }

  def delta(grad: Vector, localN: Vector, weight: Vector,
            deltaZ: Vector, deltaN: Vector): Unit = {
    deltaZ.iadd(grad)
    Ufuncs.iaxpy2(deltaN, grad, 1)
    OptFuncs.iftrldetalintersect(grad, localN, alpha)
    deltaZ.isub(grad.imul(weight))
  }

  def predict(w: Vector, v: Array[Vector], v2: Array[Vector], feature: Vector): Double = {
    val sumW = w.dot(feature)
    val f2 = feature.mul(feature)
    val sumV = v.zip(v2).map { case (v0, v2) =>
      val t1 = v0.dot(feature)
      t1*t1 - v2.dot(f2)
    }
    return sumW + sumV.sum
  }

  def predict(batch: Array[LabeledData]): Array[(Double, Double)] = {
    val indices = batch.flatMap {
      case point =>
        point.getX match {
          case intKey: IntKeyVector => intKey.getStorage
            .asInstanceOf[IntKeyVectorStorage].getIndices
        }
    }.distinct

    // fetch first
    val firsts = first.pull(Array(0, 1), indices)
    val (localZ, localN) = (firsts(0), firsts(1))
    val localW = Ufuncs.ftrlthreshold(localZ, localN, alpha, beta, lambda1, lambda2)

    // fetch second
    val seconds = second.pull((0 until dim*2).toArray, indices)
    val localV = (0 until dim).map(idx => Ufuncs.ftrlthresholdinit(seconds(idx), seconds(idx+dim),
      alpha, beta, lambda1, lambda2, 0.0, 0.01)).toArray

    val localV2 = localV.map(v => v.mul(v))

    batch.map { point =>
      val (feature, label) = (point.getX, point.getY)
      val p = predict(localW, localV, localV2, feature)
      val score = 1 / (1 + math.exp(-p))
      (label, score)
    }
  }

  def gradient(w: Vector, v: Array[Vector], v2: Array[Vector],
               label: Double, feature: Vector): (Vector, Array[Vector], Double) = {


    val marginW = w.dot(feature)
    val vdot = v.map(v0 => v0.dot(feature))
    val f2 = feature.mul(feature)
    val marginV = v.zip(vdot).zip(v2).map { case ((v0, dot),v2) =>
      dot*dot - v2.dot(f2)
    }

    val margin = -(marginW + marginV.sum)
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

  def saveWeight(path: String): Unit = {
    val format = classOf[RowIdColIdValueTextRowFormat].getCanonicalName
    val modelContext = new ModelSaveContext(path)
    val matrixContext = new MatrixSaveContext(secondName, format)
    matrixContext.addIndices((0 until dim*2).toArray)
    modelContext.addMatrix(matrixContext)
    AngelPSContext.save(modelContext)
  }

}
