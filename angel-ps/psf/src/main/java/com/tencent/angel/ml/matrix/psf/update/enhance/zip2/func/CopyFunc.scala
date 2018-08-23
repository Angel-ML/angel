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


package com.tencent.angel.ml.matrix.psf.update.enhance.zip2.func

import com.tencent.angel.ml.math2.ufuncs.expression.OpType


// todo: need improve
class CopyFunc extends Zip2MapFunc {
  setInplace(true)

  def getOpType: OpType = OpType.UNION

  def apply(ele1: Double, ele2: Double): Double = ele2

  def apply(ele1: Double, ele2: Float): Double = ele2.toDouble

  def apply(ele1: Double, ele2: Long): Double = ele2.toDouble

  def apply(ele1: Double, ele2: Int): Double = ele2.toDouble

  def apply(ele1: Float, ele2: Float): Float = ele2.toFloat

  def apply(ele1: Float, ele2: Long): Float = ele2.toFloat

  def apply(ele1: Float, ele2: Int): Float = ele2.toFloat

  def apply(ele1: Long, ele2: Long): Long = ele2.toLong

  def apply(ele1: Long, ele2: Int): Long = ele2.toInt

  def apply(ele1: Int, ele2: Int): Int = ele2.toInt
}