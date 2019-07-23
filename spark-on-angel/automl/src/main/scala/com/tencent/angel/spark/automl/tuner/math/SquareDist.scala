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


package com.tencent.angel.spark.automl.tuner.math

import breeze.generic.UFunc
import breeze.linalg.{DenseMatrix => BDM}
import breeze.linalg._

/**
  * Computes pair-wise square distances between matrices x1 and x2.
  *
  * @param x1 [N x D]
  * @param x2 [M x D]
  * @return matrix of square distances [N x M]
  */
object SquareDist extends UFunc {

  implicit object implBinary
    extends Impl2[BDM[Double], BDM[Double], BDM[Double]] {

    def apply(x1: BDM[Double],
              x2: BDM[Double]): BDM[Double] = {

      val t1 = -2.0 * (x1 * x2.t)

      val t2 = t1(*, ::) + sum(x2.t *:* x2.t, Axis._0).t

      t2(::, *) + sum(x1.t *:* x1.t, Axis._0).t
    }
  }

}