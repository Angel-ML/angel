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

import scala.util.Random

import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class PSMatrixSuite extends PSFunSuite with SharedPSContext {

  private val rows = 10
  private val cols = 10
  private val dim = 21

  test("init Dense Matrix") {
    val mat = PSMatrix.dense(rows, cols)
    val result = mat.pull()

    (0 until rows).foreach { i =>
      (0 until cols).foreach { j =>
        assert(result(i)(j) == 0.0)
      }
    }

    mat.destroy()
  }

  test("Push array") {
    val mat = PSMatrix.dense(rows, cols)
    val rand = new Random()
    val array = (0 until cols).toArray.map(x => rand.nextDouble())

    val randRow = rand.nextInt(rows)
    mat.push(randRow, array)

    val result = mat.pull()

    (0 until rows).foreach { i =>
      if (i == randRow) {
        assert(array.sameElements(result(i)))
      } else {
        assert(Array.fill(cols)(0.0).sameElements(result(i)))
      }
    }
    mat.destroy()
  }

  test("Pull row") {
    val mat = DensePSMatrix.rand(rows, cols)

    val totalMat = mat.pull()

    val rand = new Random()
    val randRow = rand.nextInt(rows)
    val oneRow = mat.pull(randRow)

    assert(oneRow.sameElements(totalMat(randRow)))
    mat.destroy()
  }

  test("increment") {
    val mat = PSMatrix.dense(rows, cols)
    val rand = new Random()
    val firstArray = (0 until cols).toArray.map(x => rand.nextDouble())
    val secondArray = (0 until cols).toArray.map(x => rand.nextGaussian())

    val rowId = rand.nextInt(rows)
    mat.increment(rowId, firstArray)
    assert(mat.pull(rowId).sameElements(firstArray))

    mat.increment(rowId, secondArray)
    val sum = (0 until cols).toArray.map(i => firstArray(i) + secondArray(i))
    assert(sum.sameElements(mat.pull(rowId)))

    mat.destroy()
  }
}
