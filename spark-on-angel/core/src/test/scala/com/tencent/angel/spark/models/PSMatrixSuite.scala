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


package com.tencent.angel.spark.models

import scala.util.Random

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.matrix.{RBIntDoubleMatrix, RBIntFloatMatrix, RBLongFloatMatrix}
import com.tencent.angel.ml.math2.vector.{IntFloatVector, LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class PSMatrixSuite extends PSFunSuite with SharedPSContext {

  private val rows = 12
  private val cols = 12

  test("init Dense Matrix") {
    val mat = PSMatrix.dense(rows, cols, RowType.T_FLOAT_DENSE)
    val result = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        assert(result.get(i, j) == 0.0f)
      }
    }
    mat.destroy()
  }

  test("init Sparse Matrix") {
    val mat = PSMatrix.sparse(rows, cols, -1, RowType.T_FLOAT_SPARSE_LONGKEY)
    val result = mat.pull().asInstanceOf[RBLongFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        assert(result.get(i, j) == 0.0f)
      }
    }
    mat.destroy()
  }

  test("create eye matrix") {
    val mat = PSMatrix.eye(rows)
    val result = mat.pull().asInstanceOf[RBIntDoubleMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (i == j)
          assert(result.get(i, j) == 1.0)
        else
          assert(result.get(i, j) == 0.0)
      }
    }
  }

  test("create diagonal matrix") {
    val arr = Array.tabulate(rows)(_ => Random.nextDouble())
    val mat = PSMatrix.diag(arr)
    val result = mat.pull().asInstanceOf[RBIntDoubleMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (i == j)
          assert(result.get(i, j) == arr(i))
        else
          assert(result.get(i, j) == 0.0)
      }
    }
  }

  test("create rand matrix") {
    val mat = PSMatrix.rand(rows, cols, RowType.T_FLOAT_DENSE, -2.0, 2.0)
    val result = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        assert(result.get(i, j) >= -2.0 && result.get(i, j) <= 2.0)
      }
    }
  }

  test("fill"){
    val mat = PSMatrix.dense(rows, cols, RowType.T_FLOAT_DENSE)
    val rowIds = Array.range(0, 5)
    val values = rowIds.map(_ => Random.nextDouble())
    mat.fill(rowIds, values)
    val results = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for(i <- 0 until rows){
      for(j <- 0 until cols){
        if(rowIds.contains(i))
          assert(math.abs(results.get(i, j) - values(i)) < 1e-6)
        else
          assert(results.get(i, j) == 0.0f)
      }
    }
  }


  test("push & update & increment by matrix") {
    val mat = PSMatrix.dense(rows, cols, RowType.T_FLOAT_DENSE)
    val initRandomMatrix = new RBIntFloatMatrix(Array.tabulate(rows)(_ =>
      VFactory.denseFloatVector(Array.tabulate(cols)(_ => Random.nextFloat()))))

    // push matrix
    mat.push(initRandomMatrix)
    val localMatrix2 = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        assert(localMatrix2.get(i, j) == initRandomMatrix.get(i, j))
      }
    }

    // increment matrix
    mat.increment(initRandomMatrix)
    val localMatrix3 = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        assert(localMatrix3.get(i, j) == 2 * initRandomMatrix.get(i, j))
      }
    }

    // update diagonal elements to 1.0f
    val updateMatrix = new RBIntFloatMatrix(Array.tabulate(rows)(i =>
      VFactory.sparseFloatVector(cols, Array(i), Array(1.0f))
    ))
    mat.update(updateMatrix)
    val localMatrix4 = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (i == j) assert(localMatrix4.get(i, j) == 1.0f)
        else assert(localMatrix3.get(i, j) == 2 * initRandomMatrix.get(i, j))
      }
    }
  }

  test("push by vector") {
    val mat = PSMatrix.rand(rows, cols, RowType.T_FLOAT_DENSE)
    val randomArrays = Array.tabulate(rows, cols) { case (_, _) => Random.nextFloat() }

    // push
    mat.push(0, VFactory.denseFloatVector(randomArrays(0)))
    mat.push(VFactory.denseFloatVector(mat.id, 1, 0, randomArrays(1)))
    mat.push(Array(2, 3), Array.tabulate(2)(i => VFactory.denseFloatVector(randomArrays(i + 2)).asInstanceOf[Vector]))

    val finalMatrix = mat.pull().asInstanceOf[RBIntFloatMatrix]
    //check push
    for (i <- 0 until 4)
      for (j <- 0 until cols)
        assert(finalMatrix.get(i, j) == randomArrays(i)(j))
  }

  test("increment by vector") {
    val mat = PSMatrix.rand(rows, cols, RowType.T_FLOAT_DENSE)
    val initRandomMatrix = mat.pull().asInstanceOf[RBIntFloatMatrix]
    val randomArrays = Array.tabulate(4, cols) { case (_, _) => Random.nextFloat() }

    // increment
    mat.increment(0, VFactory.denseFloatVector(randomArrays(0)))
    mat.increment(VFactory.denseFloatVector(mat.id, 1, 0, randomArrays(1)))
    mat.increment(Array(2, 3), Array.tabulate(2)(i => VFactory.denseFloatVector(randomArrays(i + 2)).asInstanceOf[Vector]))

    val finalMatrix = mat.pull().asInstanceOf[RBIntFloatMatrix]
    // check increment
    for(i <- 0 until 4)
      for(j <- 0 until cols)
        assert(math.abs(finalMatrix.get(i, j) - randomArrays(i)(j) - initRandomMatrix.get(i, j)) < 1e-6)

  }

  test("update by vector") {
    val mat = PSMatrix.rand(rows, cols, RowType.T_FLOAT_DENSE)
    val initRandomMatrix = mat.pull().asInstanceOf[RBIntFloatMatrix]
    val randomArrays = Array.tabulate(4, cols) { case (_, _) => Random.nextFloat() }

    // update
    val indices = Array.range(1, 8)
    mat.update(0, VFactory.sparseFloatVector(cols, indices, indices.map(i => randomArrays(0)(i))))
    mat.update(VFactory.sortedFloatVector(mat.id, 1, 0, cols, indices, indices.map(i => randomArrays(1)(i))))
    mat.update(Array(2, 3), Array.tabulate(2)(i =>
      VFactory.sparseFloatVector(cols, indices, indices.map(j => randomArrays(i + 2)(j))).asInstanceOf[Vector]
    ))

    val finalMatrix = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for(i <- 0 until 4){
      for(j <- 0 until cols){
        if(indices.contains(j))
          assert(finalMatrix.get(i, j) == randomArrays(i)(j))
        else
          assert(finalMatrix.get(i, j) == initRandomMatrix.get(i, j))
      }
    }
  }

  test("pull rows"){
    val mat = PSMatrix.dense(rows, cols, RowType.T_FLOAT_DENSE)
    val initRandomMatrix = new RBIntFloatMatrix(Array.tabulate(rows)(_ =>
      VFactory.denseFloatVector(Array.tabulate(cols)(_ => Random.nextFloat()))))
    mat.push(initRandomMatrix)

    // pull row
    var finalVector = mat.pull(0).asInstanceOf[IntFloatVector]
    for(i <- 0 until cols)
      assert(finalVector.get(i) == initRandomMatrix.get(0, i))

    // pull row with specified indices of type int
    val indices =  Array.range(3, 8)
    finalVector = mat.pull(1, indices).asInstanceOf[IntFloatVector]
    assert(finalVector.numZeros() >= cols - indices.length)
    for(i <- indices)
      assert(finalVector.get(i) == initRandomMatrix.get(1, i))

    // pull rows with specified indices of type int
    var vectors = mat.pull(Array.range(2, 5), indices).map(_.asInstanceOf[IntFloatVector])
    for(i <- vectors.indices){
      assert(vectors(i).numZeros() >= cols - indices.length)
      for(j <- indices)
        assert(vectors(i).get(j) == initRandomMatrix.get(i + 2, j))
    }

    // pull rows
    vectors = mat.pull(Array.range(5, 7)).map(_.asInstanceOf[IntFloatVector])
    for(i <- vectors.indices){
      for(j <- 0 until cols){
        assert(vectors(i).get(j) == initRandomMatrix.get(i + 5, j))
      }
    }

    // pull rows with batchSize
    vectors = mat.pull(Array.range(7, 11), 2).map(_.asInstanceOf[IntFloatVector])
    for(i <- vectors.indices){
      for(j <- 0 until cols){
        assert(vectors(i).get(j) == initRandomMatrix.get(i + 7, j))
      }
    }
  }

  test("pull with long indices"){
    val mat = PSMatrix.sparse(rows, cols, cols, RowType.T_FLOAT_SPARSE_LONGKEY)
    val indices = Array.tabulate(8)(_ => Random.nextInt(cols).toLong).distinct
    val initRandomMatrix = new RBLongFloatMatrix(Array.tabulate(rows)(_ =>
      VFactory.sparseLongKeyFloatVector(cols, indices, indices.map(_ => Random.nextFloat()))))
    mat.push(initRandomMatrix)

    // pull row with specified indices of type int
    val pulledIndices = indices.take(5)
    val finalVector = mat.pull(0, pulledIndices).asInstanceOf[LongFloatVector]
    for(i <- pulledIndices)
      assert(finalVector.get(i) == initRandomMatrix.get(0, i))

    // pull rows with specified indices of type int
    val finalVectors = mat.pull(Array.range(1, 4), pulledIndices).map(_.asInstanceOf[LongFloatVector])
    for(i <- finalVectors.indices){
      for(j <- pulledIndices)
        assert(finalVectors(i).get(j) == initRandomMatrix.get(i + 1, j))
    }
  }

  test("reset") {
    val mat = PSMatrix.dense(rows, cols, RowType.T_FLOAT_DENSE)
    val initRandomMatrix = new RBIntFloatMatrix(Array.tabulate(rows)(_ =>
      VFactory.denseFloatVector(Array.tabulate(cols)(_ => Random.nextFloat()))))
    mat.push(initRandomMatrix)

    mat.reset(0)
    mat.reset(Array.range(1, 5))
    var result = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        if (i < 5)
          assert(result.get(i, j) == 0.0f)
        else
          assert(result.get(i, j) == initRandomMatrix.get(i, j))
      }
    }
    mat.reset()
    result = mat.pull().asInstanceOf[RBIntFloatMatrix]
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        assert(result.get(i, j) == 0.0f)
      }
    }
  }

  test("destroy matrix"){
    val mat = PSMatrix.dense(rows, cols, RowType.T_FLOAT_DENSE)
    assert(!mat.deleted)
    mat.destroy()
    assert(mat.deleted)
  }
}