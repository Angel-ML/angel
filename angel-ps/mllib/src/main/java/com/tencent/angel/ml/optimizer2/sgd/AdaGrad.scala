package com.tencent.angel.ml.optimizer2.sgd

import java.util

import com.tencent.angel.ml.math.TUpdate
import com.tencent.angel.ml.optimizer2.utils.oputils._
import com.tencent.angel.ml.optimizer2.utils.{ExecuteUtils, OptUtils}
import com.tencent.angel.ml.optimizer2.{OptModel, Optimizer}
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConversions._

// https://zh.gluon.ai/chapter_optimization/adagrad-scratch.html

class AdaGrad(batchSize:Int, numUpdatePerEpoch:Int, lr:Double,
              val l1Reg:Map[String, Double]=null, val l2Reg:Map[String, Double]=null)
  extends Optimizer(batchSize, numUpdatePerEpoch, lr) {
  private val LOG = LogFactory.getLog(classOf[AdaGrad])
  private var sumGrad2: util.HashMap[String, TUpdate] = _

  override def updateLocal(model: OptModel, numSample: Int, iterCount:Int): Unit = {
    val top = new AdaGradExpr(lr, isInplace=true)
    val fop = new DefaultBinary(lr, isInplace=true)
    ExecuteUtils.executeScalar(grad, new ScalarExpr(alpha=1.0f/numSample, top=TOperation.Mul, isInplace=true))
    val delta = ExecuteUtils.executeBinary(grad, sumGrad2, model.getIndexFlag, top, fop)

    if (l2Reg != null) {
      val localMap = l2Reg.map{ case (name:String, lam:Double) => name -> (1.0 - lr * lam) }
      val oneMap = localParams.map{ case (name:String, _) => name -> 1.0 }.toMap
      OptUtils.ilinear(localParams, localMap, delta, oneMap)
    } else {
      OptUtils.iaxpy(localParams, delta, 1.0)
    }
  }

  override def initialLocal(model: OptModel, local:util.HashMap[String, TUpdate], global:util.HashMap[String, TUpdate]): Unit = {
    if (sumGrad2 == null && l2Reg != null) {
      sumGrad2 = OptUtils.emptyLike(global)
    } else if (sumGrad2 == null && l2Reg == null) {
      sumGrad2 = model.getZeroParams
    } else {
      val delta = OptUtils.axpy(local, global, -0.1)
      sumGrad2 = ExecuteUtils.executeScalar(delta, new ScalarExpr(2.0f, TOperation.Pow, true))
    }
  }

  override def psfHook(model: OptModel, numSample: Int): Unit = {
    val thresh = if (l1Reg != null) {
      model.getPSModels.map { case (name: String, _) =>
        if (l2Reg != null) {
          name -> lr * l1Reg(name) / (1.0 + lr * l2Reg(name))
        } else {
          name -> lr * l1Reg(name)
        }
      }
    } else null

    model.psfHook(thresh)
  }
}
