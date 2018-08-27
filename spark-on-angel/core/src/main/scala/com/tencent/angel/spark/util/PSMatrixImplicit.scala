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


package com.tencent.angel.spark.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.spark.models.matrix.PSMatrix

/**
  * additional operations for PSMatrix
  */
object PSMatrixImplicit {

  implicit class DoublePSMatrix(pSMatrix: PSMatrix) {
    def update(rowIds: Array[Int], indices: Array[Array[Int]], values: Array[Array[Double]]): Unit = {
      assertSameSize(rowIds, indices, values)
      pSMatrix.update(rowIds, indices.zip(values).map { case (k, v) =>
        assertSameSize(k, v)
        VFactory.sparseDoubleVector(pSMatrix.columns.toInt, k, v).asInstanceOf[Vector]
      })
    }

    def update(rowIds: Array[Int], indices: Array[Array[Long]], values: Array[Array[Double]]): Unit = {
      assertSameSize(rowIds, indices, values)
      pSMatrix.update(rowIds, indices.zip(values).map { case (k, v) =>
        assertSameSize(k, v)
        VFactory.sparseLongKeyDoubleVector(pSMatrix.columns, k, v).asInstanceOf[Vector]
      })
    }

    def update(triples: Array[(Int, Long, Double)]): Unit = {
      val (rowIds, indices, values) = unzipTriples(triples)
      update(rowIds, indices, values)
    }

    def increment(rowId: Int, delta: Array[Double]): Unit = {
      val vector = VFactory.denseDoubleVector(delta)
      vector.setRowId(rowId)
      pSMatrix.increment(vector)
    }

    def increment(dim: Long, keyValues: Array[(Long, Double)]) {
      val (keys, values) = keyValues.unzip
      increment(dim, keys, values)
    }

    def increment(dim: Long, keys: Array[Long], values: Array[Double]) {
      pSMatrix.increment(VFactory.sparseLongKeyDoubleVector(dim, keys, values))
    }

    def increment(triples: Array[(Int, Long, Double)]): Unit = {
      val (rowIds, indices, values) = unzipTriples(triples)
      val deltas: Array[Vector] = new Array[Vector](rowIds.length)
      rowIds.indices.foreach { i =>
        deltas(i) = VFactory.sparseLongKeyDoubleVector(
          pSMatrix.id, rowIds(i), 0, pSMatrix.columns, indices(i), values(i)
        )
      }
      pSMatrix.increment(rowIds, deltas)
    }

    def push(triples: Array[(Int, Long, Double)]): Unit = {
      val (rowIds, indices, values) = unzipTriples(triples)
      rowIds.indices.par.foreach { i =>
        pSMatrix.push(VFactory.sparseLongKeyDoubleVector(
          pSMatrix.id, rowIds(i), 0, pSMatrix.columns, indices(i), values(i))
        )
      }
    }


    private def unzipTriples(triples: Array[(Int, Long, Double)]) = {
      val map = new mutable.HashMap[Int, (ArrayBuffer[Long], ArrayBuffer[Double])]
      for ((rowId, index, value) <- triples) {
        map(rowId) = {
          val (c, v) = map.getOrElse(rowId, (new ArrayBuffer[Long](), new ArrayBuffer[Double]()))
          c += index
          v += value
          (c, v)
        }
      }
      val rowIds = map.keys.toArray
      val indices = new Array[Array[Long]](rowIds.length)
      val values = new Array[Array[Double]](rowIds.length)
      for (i <- rowIds.indices) {
        indices(i) = map(rowIds(i))._1.toArray
        values(i) = map(rowIds(i))._2.toArray
      }
      (rowIds, indices, values)
    }
  }

  private def assertSameSize(arr1: Array[_], arr2: Array[_], arr3: Array[_]): Unit = {
    require(arr1.length == arr2.length && arr2.length == arr3.length,
      s"size miss matched: (${arr1.length}, ${arr2.length}, ${arr3.length})")
  }

  private def assertSameSize(arr1: Array[_], arr2: Array[_]): Unit = {
    require(arr1.length == arr2.length, s"size miss matched: (${arr1.length}, ${arr2.length}")
  }
}
