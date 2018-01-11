/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.models.matrix

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class SparseMatrixSuite extends PSFunSuite with SharedPSContext {

  private val rows = 10
  private val cols = 10
  private val zeroArray = Array.fill(cols)(0.0)

  override def beforeAll(): Unit = {
    super.beforeAll()

  }

  test("push && increment") {
    val sparseMatrix = SparsePSMatrix(rows, cols)
    val rand = new Random()

    val points = new ArrayBuffer[(Int, Long, Double)]()

    val pairSet = new mutable.HashSet[(Int, Long)]()
    (0 until rows / 2).foreach { i =>
      val rowId = rand.nextInt(rows)
      (0 until cols / 2).foreach { j =>
        val colId = rand.nextInt(cols)
        if (!pairSet.contains(Tuple2(rowId, colId))) {
          pairSet.add(Tuple2(rowId, colId))
          points.append(Tuple3(rowId, colId, rand.nextDouble()))
        }
      }
    }

    // push
    sparseMatrix.push(points.toArray)

    var psMatrix = (0 until rows).toArray.map { rowId =>
      sparseMatrix.pull(rowId)
    }

    points.foreach { case (row, col, value) =>
      assert(value == psMatrix(row)(col))
    }


    // increment
    sparseMatrix.increment(points.toArray)

    psMatrix = (0 until rows).toArray.map { rowId =>
      sparseMatrix.pull(rowId)
    }

    points.foreach { case (row, col, value) =>
      assert(value * 2 == psMatrix(row)(col.toInt))
    }
  }

}
