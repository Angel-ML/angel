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

import java.util.{Random => JRandom}

import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector._
import com.tencent.angel.ps.storage.vector.func.{DoubleElemUpdateFunc, FloatElemUpdateFunc}

class XavierUniform(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, gain: Double, fin: Long, fout: Long) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double](math.sqrt(3.0) * gain * math.sqrt(2.0 / (fin + fout)))))

  def this(matrixId: Int, startId: Int, length: Int, gain: Double, fin: Long, fout: Long) =
    this(new MMUpdateParam(matrixId, startId, length, Array[Double](math.sqrt(3.0) * gain * math.sqrt(2.0 / (fin + fout)))))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val a = scalars(0)
    val rand = new JRandom(System.currentTimeMillis())
    rows.foreach {
      case r: ServerIntDoubleRow =>
        r.elemUpdate(new DoubleElemUpdateFunc {
          override def update(): Double = {
            rand.nextDouble() * 2 * a - a
          }
        })

      case r: ServerLongDoubleRow =>
        r.elemUpdate(new DoubleElemUpdateFunc {
          override def update(): Double = {
            rand.nextDouble() * 2 * a - a
          }
        })

      case r: ServerIntFloatRow =>
        r.elemUpdate(new FloatElemUpdateFunc {
          override def update(): Float = {
            (rand.nextFloat() * 2 * a - a).toFloat
          }
        })

      case r: ServerLongFloatRow =>
        r.elemUpdate(new FloatElemUpdateFunc {
          override def update(): Float = {
            (rand.nextFloat() * 2 * a - a).toFloat
          }
        })
    }
  }
}
