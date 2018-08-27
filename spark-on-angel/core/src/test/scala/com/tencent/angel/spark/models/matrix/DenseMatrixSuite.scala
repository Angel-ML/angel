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


package com.tencent.angel.spark.models.matrix

import java.util.Random

import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.spark.models.CompatibleImplicit._
import com.tencent.angel.ml.math2.vector.IntDoubleVector
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class DenseMatrixSuite extends PSFunSuite with SharedPSContext {

  private val rows = 10
  private val cols = 10
  private val dim = 21
  private val zeroArray = Array.fill(cols)(0.0)

  override def beforeAll(): Unit = {
    super.beforeAll()

  }

  test("init zero") {
    val mat = PSMatrix.zero(rows, cols)
    val result = mat.pull()
    (0 until rows).foreach { i =>
      assert(result(i).sameElements(zeroArray))
    }
    mat.destroy()
  }

  test("random matrix") {
    val mat = PSMatrix.rand(rows, cols)
    val result = mat.pull()

    (0 until rows).foreach { i =>
      (0 until cols).foreach { j =>
        assert(result(i)(j) > 0.0 && result(i)(j) < 1.0)
      }
    }
    mat.destroy()
  }

  test("eye matrix") {
    val mat = PSMatrix.eye(dim)
    val result = mat.pull()

    (0 until dim).foreach { i =>
      (0 until dim).foreach { j =>
        if (i == j) {
          assert(result(i)(j) == 1.0)
        } else {
          assert(result(i)(j) == 0.0)
        }
      }
    }
    mat.destroy()
  }

  test("init diag matrix") {
    val rand = new Random(41)
    val diag = (0 until dim).toArray.map(_ => rand.nextDouble())
    val diagMatrix = PSMatrix.diag(diag)

    val result = diagMatrix.pull()

    (0 until dim).foreach { i =>
      (0 until dim).foreach { j =>
        if (i == j) {
          assert(result(i)(j) == diag(i))
        } else {
          assert(result(i)(j) == 0.0)
        }
      }
    }
    diagMatrix.destroy()
  }

  test("fill matrix") {
    val mat = PSMatrix.fill(rows, cols, 3.14)
    val result = mat.pull()
    val expectedArray = Array.fill(cols)(3.14)
    (0 until rows).foreach { i =>
      assert(result(i).sameElements(expectedArray))
    }
    mat.destroy()
  }

  test("push") {
    val rand = new Random()
    val data = (0 until rows).flatMap(_ => (0 until cols).map(_ => rand.nextDouble())).toArray
    val localMat = MFactory.denseDoubleMatrix(rows, cols, data)
    val mat = PSMatrix.dense(rows, cols)
    mat.push(localMat)
    Thread.sleep(5000)
    //@Todo: ?????
    val result = mat.pull()
    (0 until rows).foreach { i =>
      assert(result(i).sameElements(localMat(i)))
    }
    mat.destroy()
  }

  test("increment") {
    val rand = new Random()
    val mat = PSMatrix.rand(rows, cols)
    val data = (0 until rows).toArray.map(i =>
      VFactory.denseDoubleVector(mat.id, i, 0, (0 until cols).map(_ => rand.nextDouble()).toArray))
    val randMat = mat.pull()
    val localMat = MFactory.rbIntDoubleMatrix(rows, cols, data)
    mat.increment(localMat)

    val sum = mat.pull()
    (0 until rows).foreach { i =>
      (0 until cols).foreach { j =>
        assert(sum(i)(j) == localMat(i)(j) + randMat(i)(j))
      }
    }
    mat.destroy()
  }

  test("pull with rows") {
    val rand = new Random(41)
    val diag = (0 until dim).toArray.map(_ => rand.nextDouble())
    val diagMatrix = PSMatrix.diag(diag)

    val selectRows = Array(0, dim / 2, dim / 3, dim - 1)

    val result = diagMatrix.pull(selectRows)

    result.zipWithIndex.foreach { case (row: IntDoubleVector, index) =>
      row.getStorage.getValues.zipWithIndex.foreach { case (value, id) =>
        if (id != index) {
          assert(value == 0.0)
        } else {
          assert(row.getStorage.get(index) == diag(index))
        }
      }
    }

  }
}
