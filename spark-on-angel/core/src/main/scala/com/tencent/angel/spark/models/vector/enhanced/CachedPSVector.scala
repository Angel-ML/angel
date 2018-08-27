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


package com.tencent.angel.spark.models.vector.enhanced

import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.spark.models.vector.{ConcretePSVector, VectorCacheManager}

/**
  * CachedPSVector implements a more efficient Vector, which can benefit multi tasks on one executor
  */

private[spark] class CachedPSVector(component: ConcretePSVector) extends PSVectorDecorator(component) {

  override val dimension: Long = component.dimension
  override val id: Int = component.id
  override val poolId: Int = component.poolId
  override val rowType: RowType = component.rowType

  /**
    * pull from local cache if local cache is available, otherwise, pull from ps
    */
  override def pull(): Vector = {
    if (!VectorCacheManager.pullCache.contains((poolId, id))) {
      VectorCacheManager.pullCache.synchronized {
        if (!VectorCacheManager.pullCache.contains((poolId, id))) {
          val local = component.pull()
          VectorCacheManager.pullCache.put((poolId, id), local)
        }
      }
    }
    VectorCacheManager.pullCache((poolId, id))
  }

  /**
    * pull ps vector directly from ps
    */
  def pullSkipCache(): Vector = this.component.pull()

  /**
    * Increment a local vector to cache
    */
  def increment(delta: Vector, cacheId: Int = 0): Unit = {
    aggregateCache(delta, cacheId, VectorCacheManager.INCREMENT)
  }

  /**
    * merge a local vector to cache by max
    *
    * @param vector  local vector
    * @param cacheId cacheId to distinguish different caches
    */
  def max(vector: Vector, cacheId: Int = 0): Unit = {
    aggregateCache(vector, cacheId, VectorCacheManager.MAX)
  }

  /**
    * merge a local vector to cache by min
    *
    * @param vector  local vector
    * @param cacheId cacheId to distinguish different caches
    */
  def min(vector: Vector, cacheId: Int = 0): Unit = {
    aggregateCache(vector, cacheId, VectorCacheManager.MIN)
  }

  def aggregateCache(vector: Vector,
                     cacheId: Int = 0,
                     mergeOpId: Int,
                     userDefinedMergeOp: Option[(Vector, Vector) => Vector] = None): Unit = {
    VectorCacheManager.aggregateCache(vector, VectorCacheManager.CacheKey(cacheId, poolId, id, mergeOpId), userDefinedMergeOp)
  }

  def flushIncrement(cacheId: Int = 0): Unit = flush(cacheId, VectorCacheManager.INCREMENT)

  def flushMax(cacheId: Int = 0): Unit = flush(cacheId, VectorCacheManager.MAX)

  def flushMin(cacheId: Int = 0): Unit = flush(cacheId, VectorCacheManager.MIN)

  def flush(cacheId: Int = 0, mergeFuncId: Int, userDefinedUpdateOp: Option[Vector => UpdateFunc] = None): Unit = {
    VectorCacheManager.flush(VectorCacheManager.CacheKey(cacheId, poolId, id, mergeFuncId), userDefinedUpdateOp)
  }

  override def delete(): Unit = component.delete()
}
