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


package com.tencent.angel.spark.ml.automl.math

import breeze.linalg.DenseMatrix
import breeze.linalg.inv
import breeze.linalg.diag
import breeze.linalg.sum
import breeze.numerics.log

object BreezeOp {

  /**
    * calculate the inverse of a matrix with cholesky decomposition
    * @param L: the Cholesky decomposition of matrix A where A = L'*L
    * @return inv(A)=inv(L)*inv(L')
    */
  def choleskyInv(L: DenseMatrix[Double]): DenseMatrix[Double] = {
    val invL = inv(L)
    L * L.t
  }

  /**
    * log determinant of positive definite matrices
    * @param L
    * @return
    */
  def logDet(L: DenseMatrix[Double]): Double = {
    2 * sum(log(diag(L)))
  }

}
