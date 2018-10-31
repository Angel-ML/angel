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

import com.tencent.angel.ml.matrix.psf.update.enhance.{FullUpdateFunc, FullUpdateParam}
import com.tencent.angel.ps.storage.vector._
import com.tencent.angel.ps.storage.vector.func.{DoubleElemUpdateFunc, FloatElemUpdateFunc, IntElemUpdateFunc, LongElemUpdateFunc}

/**
  * Init a random matrix, whose value is a random value between 0.0 and 1.0.
  */
class Random(param: FullUpdateParam) extends FullUpdateFunc(param) {
  def this(matrixId:Int, low:Double, high:Double) = this(new FullUpdateParam(matrixId, Array(low, high)))

  def this() = this(null)

  override def doUpdate(rows: Array[ServerIntDoubleRow],
                        values: Array[Double]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.elemUpdate(new DoubleElemUpdateFunc {
        override def update(): Double = {
          rand.nextDouble()
        }
      })
    }
  }

  override def doUpdate(rows: Array[ServerIntFloatRow],
                        values: Array[Float]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.elemUpdate(new FloatElemUpdateFunc {
        override def update(): Float = {
          rand.nextFloat()
        }
      })
    }
  }

  override def doUpdate(rows: Array[ServerIntLongRow],
                        values: Array[Long]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.elemUpdate(new LongElemUpdateFunc {
        override def update(): Long = {
          rand.nextLong()
        }
      })
    }
  }

  override def doUpdate(rows: Array[ServerIntIntRow],
                        values: Array[Int]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.elemUpdate(new IntElemUpdateFunc {
        override def update(): Int = {
          rand.nextInt()
        }
      })
    }
  }

}
