package com.tencent.angel.ml.core.variable

import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.ml.core.network.EnvContext

trait CILSImpl {
  def doCreate[T](mCtx: MatrixContext, envCtx: EnvContext[T]): Unit

  def doInit[T](mCtx: MatrixContext, envCtx: EnvContext[T], taskFlag: Int): Unit

  def doLoad[T](mCtx: MatrixContext, envCtx: EnvContext[T], path: String): Unit

  def doSave[T](mCtx: MatrixContext, indices: Array[Int], envCtx: EnvContext[T], path: String): Unit
}
