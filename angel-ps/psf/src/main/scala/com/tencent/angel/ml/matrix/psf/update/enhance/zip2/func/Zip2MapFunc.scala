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

import io.netty.buffer.ByteBuf

import com.tencent.angel.common.Serialize
import com.tencent.angel.ml.math2.ufuncs.expression.Binary
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.Zip2Map

/**
  * `Zip2MapFunc` is a parameter of [[Zip2Map]], if you
  * want to  call a `Zip2Map` for a row in matrix, you must implement a `Zip2MapFunc` as parameter
  * of `Zip2Map`
  */
trait Zip2MapFunc extends Binary with Serialize {
  override def serialize(buf: ByteBuf) {

  }

  override def deserialize(buf: ByteBuf) {

  }

  override def bufferLen(): Int = 0

}
