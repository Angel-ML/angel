/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.ml.math.vector._
import it.unimi.dsi.fastutil.ints.{Int2DoubleOpenHashMap, Int2FloatOpenHashMap, Int2IntOpenHashMap}
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

import scala.collection.JavaConverters._

class SingleShardingData[V <: TVector](vector: V) extends ShardingData {
  val is64Key = vector.isInstanceOf[TLongDoubleVector]
  val vType = vector.getClass

  def get: TVector = vector

  override def getData(offset: Long, dimension: Long): V = {
    if (is64Key) {
      require(offset + dimension <= vector.asInstanceOf[TLongDoubleVector].getLongDim)
      if (offset == 0 && dimension == vector.asInstanceOf[TLongDoubleVector].getLongDim) {
        return vector
      }
    } else {
      require(offset < Int.MaxValue && dimension < Int.MaxValue)
      require(dimension.toInt <= vector.getDimension)
      if (offset == 0 && dimension == vector.getDimension) {
        return vector
      }
    }

    val data = vector match {
      // for dense vector
      case v: DenseIntVector => new DenseIntVector(dimension.toInt, v.getValues.slice(offset.toInt, dimension.toInt))
      case v: DenseDoubleVector => new DenseDoubleVector(dimension.toInt, v.getValues.slice(offset.toInt, dimension.toInt))
      case v: DenseFloatVector => new DenseFloatVector(dimension.toInt, v.getValues.slice(offset.toInt, dimension.toInt))
      // for sparse vector
      case v: SparseIntVector => {
        val map = new Int2IntOpenHashMap()
        map.putAll(v.getIndexToValueMap.asScala.filterKeys(key => key >= offset && key < (offset + dimension)).asJava)
        new SparseIntVector(dimension.toInt, map)
      }
      case v: SparseDoubleVector => {
        val map = new Int2DoubleOpenHashMap()
        map.putAll(v.getIndexToValueMap.asScala.filterKeys(key => key >= offset && key < (offset + dimension)).asJava)
        new SparseDoubleVector(dimension.toInt, map)
      }
      case v: SparseFloatVector => {
        val map = new Int2FloatOpenHashMap()
        map.putAll(v.getIndexToValueMap.asScala.filterKeys(key => key >= offset && key < (offset + dimension)).asJava)
        new SparseFloatVector(dimension.toInt, map)
      }
      case v: SparseLongKeyDoubleVector => {
        val map = new Long2DoubleOpenHashMap()
        map.putAll(v.getIndexToValueMap.asScala.filterKeys(key => key >= offset && key < (offset + dimension)).asJava)
        new SparseLongKeyDoubleVector(dimension.toInt, map)
      }
      // for dummy vector
      case v: SparseDummyVector => {
        val idxs = v.getIndices.filter(idx => offset <= idx && offset + dimension > idx)
        new SparseDummyVector(idxs, dimension.toInt)
      }
      case v: SparseLongKeyDummyVector => {
        val idxs = v.getIndices.filter(idx => offset <= idx && offset+dimension > idx)
        new SparseLongKeyDummyVector(idxs, dimension.toInt)
      }
      // for sorted vector
      case v: SparseDoubleSortedVector =>
      case v: SparseLongKeySortedDoubleVector =>
      case _ => throw new AngelException(s"unsupported type:$vType")
    }
    data.asInstanceOf[V]
  }

}
