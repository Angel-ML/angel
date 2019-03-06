package com.tencent.angel.ml.core.network.layers.verge


import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.{Graph, TransFunc}
import com.tencent.angel.ml.core.network.layers.{InputLayer, Trainable}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys}
import com.tencent.angel.ml.core.variable.{MatVariable, Variable}
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector}
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.JField
import org.json4s.JsonDSL._

import scala.util.Random



class KmeansInputLayer(name: String,
                       outputDim: Int,
                       transFunc: TransFunc,
                       override val optimizer: Optimizer)(implicit graph: Graph)
  extends InputLayer(name, outputDim) with Trainable with Serializable {
  graph.addTrainableLayer(this)

  private val LOG = LogFactory.getLog(classOf[KmeansInputLayer])

  private val formatClassName = SharedConf.get().getString(
    MLCoreConf.ML_SIMPLEINPUTLAYER_MATRIX_OUTPUT_FORMAT,
    MLCoreConf.DEFAULT_ML_SIMPLEINPUTLAYER_MATRIX_OUTPUT_FORMAT)
  private val center: MatVariable = graph.provider.getMatVariable(s"${name}_center", outputDim,
    SharedConf.indexRange, optimizer, formatClassName, allowPullWithIndex = true)
  private val indexRange = SharedConf.indexRange
  private val C = SharedConf.get().getDouble(
    MLCoreConf.KMEANS_C,
    MLCoreConf.DEFAULT_KMEANS_C)
  
  override protected def doForward(input: Matrix): Matrix = {
    val centerDist = center.mul(center).sum(1)
    val rowNum = input.getNumRows

    input match {
      case mat if mat.isInstanceOf[BlasDoubleMatrix] || mat.isInstanceOf[RBIntDoubleMatrix]  =>
        val centerId = VFactory.denseDoubleVector(rowNum)
        val modelout = MFactory.denseDoubleMatrix(rowNum, indexRange.toInt + 1,
          new Array[Double](rowNum * (indexRange.toInt + 1)))
        val delta = MFactory.denseDoubleMatrix(rowNum, indexRange.toInt)
        for (i <- 0 until rowNum) {
          val x = mat.getRow(i)
          val len = x.dot(x)
          val dists = centerDist.sub(center.mul(x).mul(2).sum(1)).add(len)
          val id = dists.asInstanceOf[IntDoubleVector].argmin()

          centerId.set(i, id)
          delta.setRow(i, x.sub(center.getRow(id)))
        }
        modelout.setCol(0, centerId)
        for (i <- 1 until indexRange.toInt + 1) {
          modelout.setCol(i, delta.getCol(i - 1))
        }
        transFunc(modelout)

      case mat if mat.isInstanceOf[BlasFloatMatrix] || mat.isInstanceOf[RBIntFloatMatrix]  =>
        val centerId = VFactory.denseFloatVector(rowNum)
        val modelout = MFactory.denseFloatMatrix(rowNum, indexRange.toInt + 1,
          new Array[Float](rowNum * (indexRange.toInt + 1)))
        val delta = MFactory.denseDoubleMatrix(rowNum, indexRange.toInt)
        for (i <- 0 until rowNum) {
          val x = mat.getRow(i)
          val len = x.dot(x)
          val dists = centerDist.sub(center.mul(x).mul(2).sum(1)).add(len)
          val id = dists.asInstanceOf[IntFloatVector].argmin()

          centerId.set(i, id)
          delta.setRow(i, x.sub(center.getRow(id)))
        }
        modelout.setCol(0, centerId)
        for (i <- 1 until indexRange.toInt + 1) {
          modelout.setCol(i, delta.getCol(i - 1))
        }
        transFunc(modelout)
    }
  }


  override protected def doBackward(input: Matrix, gradInput: Matrix): Unit = {
    val transBack = transFunc.calGrad(forward(), gradInput)
    val v = new Array[Int](outputDim)
    val rowNum = input.getNumRows
    val colNum = input.getRow(0).dim().toInt
    input match {
      case mat if mat.isInstanceOf[BlasDoubleMatrix]  =>
        val deltaDist = MFactory.denseDoubleMatrix(outputDim, colNum)
        val delta =  MFactory.denseDoubleMatrix(rowNum, colNum)
        val centerId = transBack.getCol(0).asInstanceOf[IntDoubleVector]
        for (i <- 0 until colNum) {
          delta.setCol(i, transBack.getCol(i + 1))
        }

        for (i <- 0 until delta.getNumRows) {
          v(centerId.get(i).toInt) += 1
          if (deltaDist.getRow(centerId.get(i).toInt).sum() == 0) {
            deltaDist.setRow(centerId.get(i).toInt, delta.getRow(i))
          } else {
            deltaDist.getRow(centerId.get(i).toInt).iadd(delta.getRow(i))
          }
        }
        delta.clear()
        val vCount = VFactory.denseIntVector(v)
        val deltaCenter = Ufuncs.divnonzero(deltaDist.mul(C), vCount.add(C), true)
        variableManager.putSlot(center.asInstanceOf[Variable], deltaCenter)

      case mat if mat.isInstanceOf[BlasFloatMatrix]  =>
        val deltaDist = MFactory.denseFloatMatrix(outputDim, colNum)
        val delta =  MFactory.denseFloatMatrix(rowNum, colNum)
        val centerId = transBack.getCol(0).asInstanceOf[IntFloatVector]
        for (i <- 0 until colNum) {
          delta.setCol(i, transBack.getCol(i + 1))
        }

        for (i <- 0 until delta.getNumRows) {
          v(centerId.get(i).toInt) += 1
          if (deltaDist.getRow(centerId.get(i).toInt).sum() == 0) {
            deltaDist.setRow(centerId.get(i).toInt, delta.getRow(i))
          } else {
            deltaDist.getRow(centerId.get(i).toInt).iadd(delta.getRow(i))
          }
        }
        delta.clear()
        val vCount = VFactory.denseIntVector(v)
        val deltaCenter = Ufuncs.divnonzero(deltaDist.mul(C), vCount.add(C), true)
        variableManager.putSlot(center.asInstanceOf[Variable], deltaCenter)

      case mat if mat.isInstanceOf[RBIntDoubleMatrix]  =>
        val deltaDist = MFactory.rbIntDoubleMatrix(outputDim, colNum)
        for (i <- 0 until outputDim) {
          deltaDist.setRow(i, VFactory.denseDoubleVector(colNum))
        }
        val delta =  MFactory.denseDoubleMatrix(rowNum, colNum)
        val centerId = transBack.getCol(0).asInstanceOf[IntDoubleVector]
        for (i <- 0 until colNum) {
          delta.setCol(i, transBack.getCol(i + 1))
        }

        for (i <- 0 until delta.getNumRows) {
          v(centerId.get(i).toInt) += 1
          if (deltaDist.getRow(centerId.get(i).toInt).sum() == 0) {
            deltaDist.setRow(centerId.get(i).toInt, delta.getRow(i).asInstanceOf[IntDoubleVector])
          } else {
            deltaDist.getRow(centerId.get(i).toInt).iadd(delta.getRow(i))
          }
        }

        delta.clear()
        for (i <- 0 until outputDim) {
          deltaDist.getRow(i).imul(C).idiv(C + v(i))
        }
        variableManager.putSlot(center.asInstanceOf[Variable], deltaDist)

      case mat if mat.isInstanceOf[RBIntFloatMatrix]  =>
        val deltaDist = MFactory.rbIntFloatMatrix(outputDim, colNum)
        for (i <- 0 until outputDim) {
          deltaDist.setRow(i, VFactory.denseFloatVector(colNum))
        }
        val delta =  MFactory.denseFloatMatrix(rowNum, colNum)
        val centerId = transBack.getCol(0).asInstanceOf[IntFloatVector]
        for (i <- 0 until colNum) {
          delta.setCol(i, transBack.getCol(i + 1))
        }

        for (i <- 0 until delta.getNumRows) {
          v(centerId.get(i).toInt) += 1
          if (deltaDist.getRow(centerId.get(i).toInt).sum() == 0) {
            deltaDist.setRow(centerId.get(i).toInt, delta.getRow(i).asInstanceOf[IntFloatVector])
          } else {
            deltaDist.getRow(centerId.get(i).toInt).iadd(delta.getRow(i))
          }
        }

        delta.clear()
        for (i <- 0 until outputDim) {
          deltaDist.getRow(i).imul(C).idiv(C + v(i))
        }
        variableManager.putSlot(center.asInstanceOf[Variable], deltaDist)

    }

  }

  override def toString: String = {
    s"KmeansInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.transFuncKey -> transFunc.toJson) ~
      (LayerKeys.optimizerKey -> optimizer.toJson)

    JField(name, layerJson)
  }

  /**
    * Pick up K samples as initial centers randomly, and push them to PS.
    *
    * @param dataStorage : trainning data storage, the cluster center candidates
    */
  def initKCentersRandomly(totalTask: Int, dataStorage: DataBlock[LabeledData], K: Int): Unit = {
    LOG.info(s"Task[0] Initialize cluster centers with randomly choosen " +
      "samples.")
    val start = System.currentTimeMillis()
    val rand = new Random(System.currentTimeMillis())
    val initCenter = center.copy()

    for (i <- 0 until K) {
      if (i % totalTask == 0) {
        val newCent = dataStorage.get(rand.nextInt(dataStorage.size)).getX
        initCenter match {
          case mat: BlasDoubleMatrix =>
            mat.setRow(i, newCent)
          case mat: BlasFloatMatrix =>
            mat.setRow(i, newCent)
          case mat: RBIntDoubleMatrix =>
            mat.setRow(i, newCent.asInstanceOf[IntDoubleVector])
          case mat: RBIntFloatMatrix =>
            mat.setRow(i, newCent.asInstanceOf[IntFloatVector])
        }
      }
    }
    variableManager.putSlot(center.asInstanceOf[Variable], initCenter)
    variableManager.updateALL(0, 1)

    LOG.info(s"All tasks Init cluster centers success, cost ${System.currentTimeMillis() - start}" +
      s" ms")
  }
}

