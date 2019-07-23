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


package com.tencent.angel.spark.automl.tuner.kernel

object CovarianceType extends Enumeration {

  type CovarianceType = Value

  val MATERN3 = Value("MATERN3")
  val MATERN5 = Value("MATERN5")
  val MATERN5_ISO = Value("MATERN5_ISO")
  val SQUAREEXP_ISO = Value("SQUAREEXP_ISO")

  def fromString(name: String): Covariance = {
    val covType = CovarianceType.withName(name.toUpperCase())
    fromString(covType)
  }

  def fromString(covType: CovarianceType.Value): Covariance = covType match {
    case MATERN3 => new Matern3
    case MATERN5 => new Matern5
    case MATERN5_ISO => new Matern5Iso
    case SQUAREEXP_ISO => new SquareExpIso
    case _ => new Matern5
  }
}
