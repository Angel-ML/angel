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


package com.tencent.angel.ml.matrix.psf.update.enhance.map.func

import io.netty.buffer.ByteBuf

class Expm1(inplace: Boolean) extends MapFunc {
  def this() = this(false)

  setInplace(inplace)

  override def isOrigin: Boolean = false

  override def apply(elem: Double): Double = Math.expm1(elem)

  override def apply(elem: Float): Float = Math.ceil(elem).toFloat

  override def apply(elem: Long): Long = Math.ceil(elem).toLong

  override def apply(elem: Int): Int = Math.ceil(elem).toInt

  override def serialize(buf: ByteBuf): Unit = buf.writeBoolean(inplace)

  override def deserialize(buf: ByteBuf): Unit = super.setInplace(buf.readBoolean())

  override def bufferLen(): Int = 1
}
