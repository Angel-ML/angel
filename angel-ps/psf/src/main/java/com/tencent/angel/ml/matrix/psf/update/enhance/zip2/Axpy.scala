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


package com.tencent.angel.ml.matrix.psf.update.enhance.zip2

;

import com.tencent.angel.ml.matrix.psf.update.enhance.MFUpdateParam
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.func.AxpyFunc;

/**
  * `Axpy` function is a implement of blas.axpy. That is `yId` += a * `xId`
  */
class Axpy(param: MFUpdateParam) extends Zip2Map(param) {
  def this(matrixId: Int, xId: Int, yId: Int, a: Double) =
    this(new MFUpdateParam(matrixId, Array(yId, xId, yId), new AxpyFunc(a)))

  def this() = this(null)
}
