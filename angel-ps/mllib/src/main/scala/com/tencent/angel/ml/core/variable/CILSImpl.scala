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
package com.tencent.angel.ml.core.variable

import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.mlcore.network.EnvContext

trait CILSImpl {
  def doCreate[T](mCtx: MatrixContext, envCtx: EnvContext[T]): Unit

  def doInit[T](mCtx: MatrixContext, envCtx: EnvContext[T], taskFlag: Int): Unit

  def doLoad[T](mCtx: MatrixContext, envCtx: EnvContext[T], path: String): Unit

  def doSave[T](mCtx: MatrixContext, indices: Array[Int], envCtx: EnvContext[T], path: String): Unit

  def doRelease[T](mCtx: MatrixContext, envCtx: EnvContext[T]): Unit
}
