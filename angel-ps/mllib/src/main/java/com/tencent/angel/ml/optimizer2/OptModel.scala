package com.tencent.angel.ml.optimizer2

import java.util

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.math.matrix._
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.math.{TUpdate, TVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.model.{MLModel, PSModel}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import collection.JavaConversions._
import scala.collection.mutable
import scala.math.Numeric
import scala.reflect.runtime.universe._

abstract class OptModel(conf: Configuration, _ctx: TaskContext) extends MLModel(conf, _ctx) {
  private val LOG = LogFactory.getLog(classOf[OptModel])
  private val indexRange: Long = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
  private val fetchRate: Double = 0.8

  def calLossAndUpdateGrad(x: TVector, y: Double,
                           params: util.HashMap[String, TUpdate],
                           gradient: util.HashMap[String, TUpdate]): Double

  def calPredAndLoss(x: TVector, y: Double, params: util.HashMap[String, TUpdate]): (Double, Double)

  def getZeroParams: util.HashMap[String, TUpdate] = {
    val grad: util.HashMap[String, TUpdate] = new util.HashMap[String, TUpdate]()
    val useDense = conf.get(MLConf.ML_DATA_INPUT_FORMAT) match {
      case "dummy" | "libsvm" => false
      case _ => true
    }

    getPSModels.foreach { case (name: String, psm: PSModel) =>
      val capacity = Math.max(psm.col / 100, 64).toInt
      psm.getRowType() match {
        case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_DENSE =>
          if (psm.row == 1) {
            if (useDense) {
              grad.put(name, new DenseDoubleVector(psm.col.toInt))
            } else {
              grad.put(name, new SparseDoubleVector(psm.col.toInt, capacity))
            }
          } else {
            if (useDense) {
              val mats: DenseDoubleMatrix = new DenseDoubleMatrix(psm.row, psm.col.toInt)
              (0 until psm.row).foreach(mats.initVector)
              grad.put(name, mats)
            } else {
              val mats: Array[SparseDoubleVector] = (0 until psm.row).map { _ =>
                new SparseDoubleVector(psm.col.toInt, capacity)
              }.toArray
              grad.put(name, new SparseDoubleMatrix(psm.row, psm.col.toInt, mats))
            }
          }
        case RowType.T_DOUBLE_SPARSE_LONGKEY =>
          if (psm.row == 1) {
            grad.put(name, new SparseLongKeyDoubleVector(psm.col.toInt, capacity))
          } else {
            val mats: Array[SparseLongKeyDoubleVector] = (0 until psm.row).map { _ =>
              new SparseLongKeyDoubleVector(psm.col, capacity)
            }.toArray
            grad.put(name, new SparseDoubleLongKeyMatrix(psm.row, psm.col, mats))
          }
        case RowType.T_FLOAT_DENSE | RowType.T_FLOAT_SPARSE =>
          if (psm.row == 1) {
            if (useDense) {
              grad.put(name, new DenseFloatVector(psm.col.toInt))
            } else {
              grad.put(name, new SparseFloatVector(psm.col.toInt, capacity))
            }
          } else {
            if (useDense) {
              val mats: DenseDoubleMatrix = new DenseDoubleMatrix(psm.row, psm.col.toInt)
              (0 until psm.row).foreach(mats.initVector)
              grad.put(name, mats)
            } else {
              val mats: Array[SparseFloatVector] = (0 until psm.row).map { _ =>
                new SparseFloatVector(psm.col.toInt, capacity)
              }.toArray
              grad.put(name, new SparseFloatMatrix(psm.row, psm.col.toInt, mats))
            }
          }
        case _ => throw new AngelException("Row Type is not support!")
      }
    }

    grad
  }

  def pullParamsFromPS[N: Numeric : TypeTag](indexes: Array[N], flag: util.HashMap[String, Boolean]): util.HashMap[String, TUpdate] = {
    val params: util.HashMap[String, TUpdate] = new util.HashMap[String, TUpdate]()

    getPSModels.foreach { case (name: String, psm: PSModel) =>
      LOG.info(s"Start to pull $name from PS ...")
      if (psm.row == 1) {
        // for vector
        val vect = typeOf[N] match {
          case t if t == typeOf[Int] =>
            if (indexes != null && flag(name) && 1.0 * indexes.length / indexRange < fetchRate) {
              psm.getRowWithIndex(0, indexes.asInstanceOf[Array[Int]])
            } else {
              psm.getRow(0)
            }
          case t if t == typeOf[Long] =>
            if (indexes != null && flag(name) && 1.0 * indexes.length / indexRange < fetchRate) {
              psm.getRowWithLongIndex(0, indexes.asInstanceOf[Array[Long]])
            } else {
              psm.getRow(0)
            }
        }
        params.put(name, vect)
      } else {
        // for matrix
        psm.getRowType() match {
          case RowType.T_DOUBLE_SPARSE =>
            val mat = new SparseDoubleMatrix(psm.row, psm.col.toInt)
            (0 until psm.row).foreach { ridx =>
              if (indexes != null && flag(name) && 1.0 * indexes.length / indexRange < fetchRate) {
                mat.setRow(ridx, psm.getRowWithIndex(ridx, indexes.asInstanceOf[Array[Int]]).asInstanceOf[SparseDoubleVector])
              } else {
                mat.setRow(ridx, psm.getRow(ridx).asInstanceOf[SparseDoubleVector])
              }
            }
            params.put(name, mat)
          case RowType.T_DOUBLE_DENSE =>
            val mat = new DenseDoubleMatrix(psm.row, psm.col.toInt)
            (0 until psm.row).foreach { ridx =>
              mat.setRow(ridx, psm.getRow(ridx).asInstanceOf[DenseDoubleVector])
            }
            params.put(name, mat)
          case RowType.T_DOUBLE_SPARSE_LONGKEY =>
            val mat = new SparseDoubleLongKeyMatrix(psm.row, psm.col)
            (0 until psm.row).foreach { ridx =>
              if (indexes != null && flag(name) && 1.0 * indexes.length / indexRange < fetchRate) {
                mat.setRow(ridx, psm.getRowWithLongIndex(ridx, indexes.asInstanceOf[Array[Long]]).asInstanceOf[SparseLongKeyDoubleVector])
              } else {
                mat.setRow(ridx, psm.getRow(ridx).asInstanceOf[SparseLongKeyDoubleVector])
              }
            }
            params.put(name, mat)
          case RowType.T_FLOAT_DENSE =>
            val mat = new DenseFloatMatrix(psm.row, psm.col.toInt)
            (0 until psm.row).foreach { ridx =>
              mat.setRow(ridx, psm.getRow(ridx).asInstanceOf[DenseFloatVector])
            }
            params.put(name, mat)
          case RowType.T_FLOAT_SPARSE =>
            val mat = new SparseFloatMatrix(psm.row, psm.col.toInt)
            (0 until psm.row).foreach { ridx =>
              if (indexes != null && flag(name) && 1.0 * indexes.length / indexRange < fetchRate) {
                mat.setRow(ridx, psm.getRowWithIndex(ridx, indexes.asInstanceOf[Array[Int]]).asInstanceOf[SparseFloatVector])
              } else {
                mat.setRow(ridx, psm.getRow(ridx).asInstanceOf[SparseFloatVector])
              }
            }
            params.put(name, mat)
          case _ => throw new AngelException("Row Type is not support!")
        }
      }
      LOG.info(s"$name is pulled !")
    }

    params
  }

  def pushParamsToPS(detla: util.HashMap[String, TUpdate]): Unit = {
    getPSModels.foreach { case (name: String, psm: PSModel) =>
      detla(name) match {
        case vect: TVector =>
          psm.increment(0, vect)
        case mat: RowbaseMatrix[_] =>
          (0 until psm.row).foreach { ridx => psm.increment(ridx, mat.getRow(ridx)) }
      }
    }

    getPSModels.foreach { case (name: String, psm: PSModel) =>
      LOG.info(s"Start to push $name from PS ...")
      psm.syncClock()
      LOG.info(s"$name is pushed !")
    }
  }

  def initModels[N: Numeric : TypeTag](indexes: Array[N]): Unit

  def psfHook(thresh: mutable.Map[String, Double]): Unit

  def getIndexFlag: util.HashMap[String, Boolean]

  def calSparsity(params: util.HashMap[String, TUpdate]): Double = {
    var nonZeroNumber: Long = 0L
    var totalNumber: Long = 0L
    params.foreach {
      case (name, v: DenseDoubleVector) =>
        nonZeroNumber += v.nonZeroNumber()
        totalNumber += getPSModels(name).validIndexNum
      case (name, v: DenseFloatVector) =>
        nonZeroNumber += v.nonZeroNumber()
        totalNumber += getPSModels(name).validIndexNum
      case (name, v: SparseDoubleVector) =>
        nonZeroNumber += v.nonZeroNumber()
        totalNumber += getPSModels(name).validIndexNum
      case (name, v: SparseFloatVector) =>
        nonZeroNumber += v.nonZeroNumber()
        totalNumber += getPSModels(name).validIndexNum
      case (name, v: SparseLongKeyDoubleVector) =>
        nonZeroNumber += v.nonZeroNumber()
        totalNumber += getPSModels(name).validIndexNum
      case (name, v: SparseLongKeyFloatVector) =>
        nonZeroNumber += v.nonZeroNumber()
        totalNumber += getPSModels(name).validIndexNum
      case (name, m: DenseDoubleMatrix) =>
        m.getVectors.foreach { v =>
          nonZeroNumber += v.nonZeroNumber()
          totalNumber += getPSModels(name).validIndexNum
        }
      case (name, m: DenseFloatMatrix) =>
        m.getVectors.foreach { v =>
          nonZeroNumber += v.nonZeroNumber()
          totalNumber += getPSModels(name).validIndexNum
        }
      case (name, m: SparseDoubleMatrix) =>
        m.getVectors.foreach { v =>
          nonZeroNumber += v.nonZeroNumber()
          totalNumber += getPSModels(name).validIndexNum
        }
      case (name, m: SparseFloatMatrix) =>
        m.getVectors.foreach { v =>
          nonZeroNumber += v.nonZeroNumber()
          totalNumber += getPSModels(name).validIndexNum
        }
      case (name, m: SparseDoubleLongKeyMatrix) =>
        m.getVectors.foreach { v =>
          nonZeroNumber += v.nonZeroNumber()
          totalNumber += getPSModels(name).validIndexNum
        }
    }

    1.0 * nonZeroNumber / totalNumber
  }

  protected def initBiasModel(vectModel: PSModel): Unit = {
    vectModel.getRowType() match {
      case RowType.T_DOUBLE_DENSE | RowType.T_FLOAT_DENSE =>
        vectModel.zero()
      case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_SPARSE_LONGKEY =>
        val bias_init = vectModel.getRow(0).asInstanceOf[TDoubleVector]
        bias_init.set(0, 0.0)
        vectModel.increment(0, bias_init)
      case RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_SPARSE_LONGKEY =>
        val bias_init = vectModel.getRow(0).asInstanceOf[TFloatVector]
        bias_init.set(0, 0.0f)
        vectModel.increment(0, bias_init)
      case _ => throw new AngelException("RowType is not support!")
    }
  }

  protected def initVectorModel[N: Numeric : TypeTag](vectModel: PSModel, indexes: Array[N], vStddev: Double): Unit = {
    vectModel.getRowType() match {
      case RowType.T_DOUBLE_DENSE | RowType.T_FLOAT_DENSE =>
        vectModel.update(new RandomNormal(vectModel.getMatrixId(), 0, 0.0, vStddev)).get()
      case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_SPARSE_LONGKEY
           | RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_SPARSE_LONGKEY =>
        val v_init = vectModel.getRow(0)
        initVector(v_init, indexes, vStddev)
        vectModel.increment(0, v_init)
      case _ => throw new AngelException("Matrix initial failure!")
    }
  }

  private def initVector[N: Numeric : TypeTag](v_init: TVector, indexes: Array[N], vStddev: Double): Unit = {
    v_init.getType match {
      case RowType.T_DOUBLE_SPARSE =>
        val dv = v_init.asInstanceOf[SparseDoubleVector]
        if (indexes != null && indexes.nonEmpty) {
          indexes.asInstanceOf[Array[Int]].foreach { cidx =>
            dv.set(cidx, vStddev * Math.random())
          }
        } else {
          (0 until v_init.getDimension).foreach { cidx =>
            dv.set(cidx, vStddev * Math.random())
          }
        }
      case RowType.T_DOUBLE_SPARSE_LONGKEY =>
        val dlv = v_init.asInstanceOf[SparseLongKeyDoubleVector]
        if (indexes != null && indexes.nonEmpty) {
          indexes.asInstanceOf[Array[Long]].foreach { cidx =>
            dlv.set(cidx, vStddev * Math.random())
          }
        } else {
          (0L until dlv.getLongDim).foreach { cidx =>
            dlv.set(cidx, vStddev * Math.random())
          }
        }
      case RowType.T_FLOAT_SPARSE =>
        val fv = v_init.asInstanceOf[SparseFloatVector]
        if (indexes != null && indexes.nonEmpty) {
          indexes.asInstanceOf[Array[Int]].foreach { cidx =>
            fv.set(cidx, (vStddev * Math.random()).toFloat)
          }
        } else {
          (0 until fv.getDimension).foreach { cidx =>
            fv.set(cidx, (vStddev * Math.random()).toFloat)
          }
        }
      case RowType.T_FLOAT_SPARSE_LONGKEY =>
        val flv = v_init.asInstanceOf[SparseLongKeyFloatVector]
        if (indexes != null && indexes.nonEmpty) {
          indexes.asInstanceOf[Array[Long]].foreach { cidx =>
            flv.set(cidx, (vStddev * Math.random()).toFloat)
          }
        } else {
          (0L until flv.getLongDim).foreach { cidx =>
            flv.set(cidx, (vStddev * Math.random()).toFloat)
          }
        }
      case _ => throw new AngelException("type is not support!")
    }
  }

  protected def initMatrixModel[N: Numeric : TypeTag](matModel: PSModel, indexes: Array[N], vStddev: Double): Unit = {
    matModel.getRowType() match {
      case RowType.T_DOUBLE_DENSE | RowType.T_FLOAT_DENSE =>
        matModel.update(new RandomNormal(matModel.getMatrixId(), 0, matModel.row, 0.0, vStddev)).get()
      case RowType.T_DOUBLE_SPARSE | RowType.T_DOUBLE_SPARSE_LONGKEY
           | RowType.T_FLOAT_SPARSE | RowType.T_FLOAT_SPARSE_LONGKEY =>
        (0 until matModel.row).foreach { vidx =>
          val v_init = matModel.getRow(vidx)
          initVector(v_init, indexes, vStddev)
          matModel.increment(vidx, v_init)
        }
      case _ => throw new AngelException("Matrix initial failure!")
    }
  }
}
