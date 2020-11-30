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
package com.tencent.angel.graph.client.node2vec.getfuncs.getdegreebucket;

import com.tencent.angel.graph.client.node2vec.getfuncs.getmindegree.PullMinDegreePartitionResult;
import com.tencent.angel.graph.client.node2vec.getfuncs.getmindegree.PullMinDegreeResult;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import com.tencent.angel.ps.storage.vector.storage.LongIntVectorStorage;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;

public class PullDegreeBucket extends GetFunc {
  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public PullDegreeBucket(GetParam param) {
    super(param);
  }

  public PullDegreeBucket() {
    super(null);
  }

  private int getBucketId(int key, int maxDegree, int minDegree, int numBuckets) {
    if (key == maxDegree) {
      return numBuckets - 1;
    } else {
      return (int) Math.floor(1.0 * numBuckets * (key - minDegree) / (maxDegree - minDegree));
    }
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PullDegreeBucketPartitionParam pparam = (PullDegreeBucketPartitionParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);

    int maxDegree = pparam.getMaxDegree();
    int minDegree = pparam.getMinDegree();
    int numBuckets = pparam.getNumBuckets();

    Int2IntOpenHashMap partResult = new Int2IntOpenHashMap();
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      LongArrayElement value = (LongArrayElement) entry.getValue();
      int degree = value.getData().length;
      int bucketId = getBucketId(degree, maxDegree, minDegree, numBuckets);

      if (partResult.containsKey(bucketId)) {
        partResult.addTo(bucketId, 1);
      } else {
        partResult.put(bucketId, 1);
      }
    }

    return new PullDegreeBucketPartitionResult(partResult);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2IntOpenHashMap result = new Int2IntOpenHashMap();

    for (PartitionGetResult partResult : partResults) {
      Int2IntOpenHashMap part = ((PullDegreeBucketPartitionResult) partResult).getPartResult();

      ObjectIterator<Int2IntMap.Entry> iter = part.int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int key = entry.getIntKey();
        int value = entry.getIntValue();

        if (result.containsKey(key)) {
          result.addTo(key, value);
        } else {
          result.put(key, value);
        }
      }
    }

    return new PullDegreeBucketResult(result);
  }
}
