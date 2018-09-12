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
import com.tencent.angel.ml.math2.storage.IntKeyVectorStorage
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, IntIntVector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.client.PSClient
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import org.apache.spark.rdd.RDD

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

  def buildRoutingIndex(part: Int, iter: Iterator[Array[Int]]): Iterator[(Int, Int)] = {
    val indices = iter.next()
    indices.map(f => (f, part)).iterator
  }

  def mapWithBroadcast(data: RDD[LabeledData]): (Map[Int, Int], RDD[LabeledData]) = {

    data.cache()
    data.count()

    val features = data.mapPartitions(getIndices).flatMap(f => f).map(f => (f, 1))
      .reduceByKey(_ + _).map(f => f._1).collect()
    val featureMap = features.zipWithIndex.toMap
    val bMap = data.sparkContext.broadcast(featureMap)
    val dim  = featureMap.size
    println(s"new dim=$dim")
    val newData = data.map { case point =>
      point.getX match {
        case v: IntFloatVector =>
          val indices = v.getStorage.getIndices.map(f => bMap.value(f))
          val values = v.getStorage.getValues
          val x = VFactory.sparseFloatVector(dim, indices, values)
          new LabeledData(x, point.getY)
        case v: IntDoubleVector =>
          val indices = v.getStorage.getIndices.map(f => bMap.value(f))
          val values = v.getStorage.getValues
          val x = VFactory.sparseDoubleVector(dim, indices, values)
          new LabeledData(x, point.getY)
      }
    }

    newData.cache()
    newData.count()

    data.unpersist()

    (featureMap, newData)
  }

  def updateFeatureMapPS(iter: Iterator[(Int, Long)], matrixId: Int, dim: Int): Iterator[Int] = {
    PSClient.instance()
    val update = VFactory.sparseIntVector(dim)
    while (iter.hasNext) {
      val (feature, index) = iter.next()
      update.set(feature, index.toInt)
    }

    PSMatrixUtils.incrementRow(matrixId, 0, update)
    Iterator.single(0)
  }

  def reMapFeature(iter: Iterator[LabeledData], matrixId: Int, dim: Int): Iterator[LabeledData] = {
    PSClient.instance()
    val set = new IntOpenHashSet()
    val samples = iter.toArray
    samples.foreach(f => f.getX.getStorage.asInstanceOf[IntKeyVectorStorage]
      .getIndices.map(i => set.add(i)))
    val index = VFactory.denseIntVector(set.toIntArray())
    val vector = PSMatrixUtils.getRowWithIndex(matrixId, 0, index).asInstanceOf[IntIntVector]

    val newSamples = samples.map { case point =>
      point.getX match {
        case v: IntFloatVector =>
          val indices = v.getStorage.getIndices.map(f => vector.get(f))
          val values = v.getStorage.getValues
          val x = VFactory.sparseFloatVector(dim, indices, values)
          new LabeledData(x, point.getY)
        case v: IntDoubleVector =>
          val indices = v.getStorage.getIndices.map(f => vector.get(f))
          val values = v.getStorage.getValues
          val x = VFactory.sparseDoubleVector(dim, indices, values)
          new LabeledData(x, point.getY)
      }
    }

    newSamples.iterator
  }

  def mapWithPS(data: RDD[LabeledData]): (Int, Int, RDD[LabeledData]) = {
    println(s"remapping feature start")
    data.cache()
    data.count()

    // maxFeature + 1 is the original dimension
    val maxFeature = data.mapPartitions(getIndices).flatMap(f => f).max()
    println(s"original feature number = ${maxFeature + 1}")
    // create one mapping matrix
    val ctx = PSMatrixUtils.createPSMatrixCtx("features", 1, maxFeature + 1, RowType.T_INT_SPARSE)
    val matrixId = PSMatrixUtils.createPSMatrix(ctx)

    val features = data.mapPartitions(getIndices).flatMap(f => f).map(f => (f, 1))
      .reduceByKey(_ + _).map(f => f._1)
    val featureMap = features.zipWithIndex()
    featureMap.cache()
    val dim = featureMap.count().toInt
    println(s"new dim = $dim")


    featureMap.mapPartitions(it => updateFeatureMapPS(it, matrixId, dim), true).count()

    val newData = data.mapPartitions(it => reMapFeature(it, matrixId, dim))
    newData.cache()
    newData.count()
    println(s"remapping feature finish")
    data.unpersist()

    (matrixId, dim, newData)
  }
}
