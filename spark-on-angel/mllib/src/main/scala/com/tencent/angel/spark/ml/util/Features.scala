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

package com.tencent.angel.spark.ml.util

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.{IntFloatSortedVectorStorage, IntFloatSparseVectorStorage, IntKeyVectorStorage}
import com.tencent.angel.ml.math2.vector.{IntIntVector, LongIntVector}
import com.tencent.angel.ml.matrix.RowType
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.context.PSContext

object Features {

  def getIndices(iter: Iterator[LabeledData]): Iterator[Array[Int]] = {
    SharedConf.keyType() match {
      case "int" =>
        val indices = new IntOpenHashSet()
        iter.foreach(f =>
          f.getX.getStorage.asInstanceOf[IntKeyVectorStorage].getIndices
            .foreach(i => indices.add(i))
        )
        Iterator.single(indices.toIntArray)
      case "long" =>
        null
    }
  }

  def updateSparseToDense(iter: Iterator[(Int, Long)],
                          sparseToDenseMatrixId: Int,
                          sparseDim: Long): Iterator[Int] = {
    PSContext.instance()
    val update = VFactory.sparseLongKeyIntVector(sparseDim)
    while (iter.hasNext) {
      val (feature, index) = iter.next()
      update.set(feature, index.toInt)
    }

    PSMatrixUtils.incrementRow(sparseToDenseMatrixId, 0, update)
    Iterator.single(0)
  }

  def updateDenseToSparse(iter: Iterator[(Int, Long)],
                          denseToSparseMatrixId: Int,
                          denseDim: Int): Iterator[Int] = {
    PSContext.instance()
    val update = VFactory.sparseIntVector(denseDim)
    while (iter.hasNext) {
      val (feature, index) = iter.next()
      update.set(index.toInt, feature)
    }

    PSMatrixUtils.incrementRow(denseToSparseMatrixId, 0, update)
    Iterator.single(0)
  }

  def sparseToDenseOnePartition(iter: Iterator[LabeledData], matrixId: Int, denseDim: Int): Iterator[LabeledData] = {
    PSContext.instance()
    val set = new LongOpenHashSet()
    val samples = iter.toArray
    samples.foreach(f => f.getX.getStorage.asInstanceOf[IntKeyVectorStorage]
      .getIndices.map(i => set.add(i)))
    val index = VFactory.denseLongVector(set.toLongArray())
    val vector = PSMatrixUtils.getRowWithIndex(1, matrixId, 0, index).asInstanceOf[LongIntVector]

    val newData = samples.map { case point =>
      point.getX.getStorage match {
        case s: IntFloatSortedVectorStorage =>
          val indices = s.getIndices
          var i = 0
          while (i < indices.length) {
            indices(i) = vector.get(indices(i))
            i += 1
          }
          val x = VFactory.sortedFloatVector(denseDim, indices, s.getValues)
          new LabeledData(x, point.getY)
        case s: IntFloatSparseVectorStorage =>
          val indices = s.getIndices.map(f => vector.get(f))
          val values = s.getValues
          val x = VFactory.sparseFloatVector(denseDim, indices, values)
          new LabeledData(x, point.getY)
      }
    }

    newData.iterator
  }

  def featureSparseToDense(data: RDD[LabeledData]): (Int, Int, Int, Long, RDD[LabeledData]) = {
    println(s"making feature from sparse to dense start")

    data.cache()
    data.count()

    val sparseDim = data.mapPartitions(getIndices).flatMap(f => f).max().toLong + 1
    // create sparseToDense matrix
    val sparseToDense = PSMatrixUtils.createPSMatrixCtx("sparseToDense", 1, sparseDim, RowType.T_INT_SPARSE_LONGKEY)
    val sparseToDenseMatrixId = PSMatrixUtils.createPSMatrix(sparseToDense)

    // calculate dense feature dimension
    val features = data.mapPartitions(getIndices).flatMap(f => f).map(f => (f, 1))
      .reduceByKey(_ + _).map(f => f._1)
    val featureMap = features.zipWithIndex()
    featureMap.cache()
    val denseDim = featureMap.count().toInt
    println(s"feature sparse dimension = $sparseDim, dense dimension = $denseDim")

    // create denseToSparse matrix
    val denseToSparse = PSMatrixUtils.createPSMatrixCtx("denseToSparse", 1, denseDim, RowType.T_INT_DENSE)
    val denseToSparseMatrixId = PSMatrixUtils.createPSMatrix(denseToSparse)

    // initialize sparseToDense and denseToSparse matrix
    featureMap.mapPartitions(it => updateSparseToDense(it, sparseToDenseMatrixId, sparseDim), true).count()
    featureMap.mapPartitions(it => updateDenseToSparse(it, denseToSparseMatrixId, denseDim), true).count()

    // change sparse feature to dense feature index
    val newData = data.mapPartitions(it => sparseToDenseOnePartition(it, sparseToDenseMatrixId, denseDim))
    println(s"feature sparse to dense finish")

    newData.cache()
    newData.count()
    data.unpersist()

    (denseToSparseMatrixId, denseDim, sparseToDenseMatrixId, sparseDim, newData)
  }


}
