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
package com.tencent.angel.graph.client.sampleneighbor6;

import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.List;

/**
 * SampleNeighbor for some algos which needs to filter neighbors
 */
public class SampleNeighbor extends GetFunc {

  private Long2IntOpenHashMap srcIndex;
  private Long2IntOpenHashMap dstIndex;
  private LongArrayList srcs;
  private LongArrayList dsts;
  private IntArrayList types;
  private LongOpenHashSet secondKeys;

  public SampleNeighbor(SampleNeighborParam param, Long2IntOpenHashMap srcIndex,
                        Long2IntOpenHashMap dstIndex, LongArrayList srcs, LongArrayList dsts,
                        LongOpenHashSet secondKeys, IntArrayList types) {
    super(param);
    this.srcIndex = srcIndex;
    this.dstIndex = dstIndex;
    this.srcs = srcs;
    this.dsts = dsts;
    this.secondKeys = secondKeys;
    this.types = types;
  }

  public SampleNeighbor(SampleNeighborParam param) {
    this(param, null, null, null, null, null, null);
  }

  public SampleNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    SampleNeighborPartParam param = (SampleNeighborPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);
    return new SampleNeighborPartResult(param.getPartKey().getPartitionId(),
      row, param.getKeys(), param.getWithoutNeighKeys(), param.getContainNeighKeys(), param.getNumSample(), param.getSampleTypes());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    SampleNeighborParam param = (SampleNeighborParam) getParam();
    long[] keys = param.getKeys();

        Int2ObjectArrayMap<PartitionGetResult> partIdToResult = new Int2ObjectArrayMap<>();
    for (PartitionGetResult result: partResults)
      partIdToResult.put(((SampleNeighborPartResult) result).getPartId(), result);

    for (PartitionGetParam partParam: param.getParams()) {
      SampleNeighborPartParam param0 = (SampleNeighborPartParam) partParam;
      int start = param0.getStartIndex();
      int end = param0.getEndIndex();
      SampleNeighborPartResult result = (SampleNeighborPartResult) partIdToResult.get(param0.getPartKey().getPartitionId());
      int[] indptr = result.getIndptr();
      long[] neighbors = result.getNeighbors();

      int[] sampleTypes = result.getTypes();

      assert indptr.length == (end - start) + 1;
      if (indptr != null) {
        for (int i = start; i < end; i++) {
          int keyIndex = srcIndex.get(keys[i]);
          for (int j = indptr[i - start]; j < indptr[i - start + 1]; j++) {
            long n = neighbors[j];
            if (!dstIndex.containsKey(n)) {
              dstIndex.put(n, dstIndex.size());
            }
            secondKeys.add(n);
            srcs.add(keyIndex);
            dsts.add(dstIndex.get(n));
          }

          if (param.getSampleTypes()) {
            for (int j = indptr[i - start]; j < indptr[i - start + 1]; j++)
              types.add(sampleTypes[j]);
          }
        }
      }
    }

    return new ScalarAggrResult(0);
  }

}
