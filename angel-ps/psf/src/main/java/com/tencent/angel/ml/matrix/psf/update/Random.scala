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

import com.tencent.angel.ml.matrix.psf.update.enhance.FullUpdateFunc
import com.tencent.angel.ps.storage.vector._

/**
  * Init a random matrix, whose value is a random value between 0.0 and 1.0.
  */
class Random(matrixId: Int) extends FullUpdateFunc(matrixId, Array[Double]()) {
  def this() = this(-1)

  override def doUpdate(rows: Array[ServerIntDoubleRow],
                        values: Array[Double]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.startWrite()
      try {
        val data = row.getValues
        for (i <- data.indices) data(i) = rand.nextDouble()
      } finally {
        row.endWrite()
      }
    }
  }

  override def doUpdate(rows: Array[ServerIntFloatRow],
                        values: Array[Float]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.startWrite()
      try {
        val data = row.getValues
        for (i <- data.indices) data(i) = rand.nextFloat()
      } finally {
        row.endWrite()
      }
    }
  }

  override def doUpdate(rows: Array[ServerIntLongRow],
                        values: Array[Long]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.startWrite()
      try {
        val data = row.getValues
        for (i <- data.indices) data(i) = rand.nextLong()
      } finally {
        row.endWrite()
      }
    }
  }

  override def doUpdate(rows: Array[ServerIntIntRow],
                        values: Array[Int]): Unit = {
    val rand = new scala.util.Random()
    rows.foreach { row =>
      row.startWrite()
      try {
        val data = row.getValues
        for (i <- data.indices) data(i) = rand.nextInt()
      } finally {
        row.endWrite()
      }
    }
  }

}
