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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector._
import com.tencent.angel.ps.storage.vector.func._
import com.tencent.angel.ps.storage.vector.op.{IDoubleValueOp, IFloatValueOp, IIntValueOp, ILongValueOp}

/**
  * Generate a random array for `rowId`, each element belongs to normal distribution N(mean, stddev)
  */
class RandomNormal(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, mean: Double, stddev: Double) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double](mean, stddev)))

  def this(matrixId: Int, startId: Int, length: Int, mean: Double, stddev: Double) =
    this(new MMUpdateParam(matrixId, startId, length, Array[Double](mean, stddev)))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {
    val mean = scalars(0)
    val stdDev = scalars(1)
    val rand = new util.Random(System.currentTimeMillis())
    rows.foreach {
      case r: IDoubleValueOp =>
        r.elemUpdate(new DoubleElemUpdateFunc {
          override def update(): Double = {
            stdDev * rand.nextGaussian() + mean
          }
        })

      case r: IFloatValueOp =>
        r.elemUpdate(new FloatElemUpdateFunc {
          override def update(): Float = {
            (stdDev * rand.nextGaussian() + mean).toFloat
          }
        })

      case r: IIntValueOp =>
        r.elemUpdate(new IntElemUpdateFunc {
          override def update(): Int = {
            (stdDev * rand.nextGaussian() + mean).toInt
          }
        })

      case r: ILongValueOp =>
        r.elemUpdate(new LongElemUpdateFunc {
          override def update(): Long = {
            (stdDev * rand.nextGaussian() + mean).toLong
          }
        })
      case r => throw new AngelException(s"not implemented for ${r.getRowType}")
    }
  }
}

private object RandomNormal {
  private def randomNormalFill[T](mean: Double, stdDev: Double, arr: Array[T], converter: Double => T): Unit = {
    val rand = new util.Random(System.currentTimeMillis())
    arr.indices.foreach(i => arr(i) = converter(stdDev * rand.nextGaussian() + mean))
  }
}
