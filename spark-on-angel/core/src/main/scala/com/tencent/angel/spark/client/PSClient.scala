/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.client

import com.github.fommil.netlib.F2jBLAS

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ops.{Initializer, MatrixOps, SparseRowOps, VectorOps}

/**
 * PSClient is a _instance which contains operations for PSVector &  PSMatrix on the PS nodes.
 * These operations can be called on the Spark driver or executor.
 */
private[spark] class PSClient {
  private[spark] val context = PSContext.instance()
  private[spark] val BLAS = new F2jBLAS

  private[spark] val initOps = new Initializer()
  private[spark] val vectorOps = new VectorOps()
  private[spark] val sparseRowOps = new SparseRowOps()
  private[spark] val matrixOps = new MatrixOps()
}

object PSClient {
  private var _instance: PSClient = _

  private [angel] def instance(): PSClient = {
    if (_instance == null) {
      classOf[PSClient].synchronized {
        if (_instance == null) {
          _instance = new PSClient()
        }
      }
    }
    _instance
  }
}
