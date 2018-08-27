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


package com.tencent.angel.ml.matrix.psf.update

import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.psf.update.enhance.VVUpdateFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.VVUpdateFunc.VVUpdateParam

/**
 * `MaxA` is find the maximum value of each element in `rowId` row and `other`
 */
class MaxA(param: VVUpdateParam) extends VVUpdateFunc(param) {

  def this(matrixId:Int, rowId: Int, other: Vector) = this(new VVUpdateParam(matrixId, rowId, other))
  def this() = this(null)

  override def updateDouble(self: Double, other: Double): Double = math.max(self, other)

  override def updateFloat(self: Float, other: Float): Float = math.max(self, other)

  override def updateLong(self: Long, other: Long): Long = math.max(self, other)

  override def updateInt(self: Int, other: Int): Int = math.max(self, other)

}
