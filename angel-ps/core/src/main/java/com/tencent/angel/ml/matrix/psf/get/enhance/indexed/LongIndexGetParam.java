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
 *
 */

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.utils.Sort;

import java.util.*;

public class LongIndexGetParam extends GetParam {
  private int rowId;
  private volatile long[] indexes;
  private volatile Map<PartitionKey, long[]> partKeyToIndexesMap;

  /**
   * @param matId matrixID
   * @param rowId rowID
   * @param indexes specified index
   */
  public LongIndexGetParam(int matId, int rowId, long[] indexes) {
    super(matId);
    this.rowId = rowId;
    this.indexes = indexes;
  }

  /**
   * @param matId matrixID
   * @param rowId rowID
   * @param partKeyToIndexesMap specified index
   */
  public LongIndexGetParam(int matId, int rowId, Map<PartitionKey, long[]> partKeyToIndexesMap) {
    super(matId);
    this.rowId = rowId;
    this.partKeyToIndexesMap = partKeyToIndexesMap;
  }

  /**
   * Find the used partition of the specifiex index array of this matrix this row
   * @return partition get param of specified index
   */
  @Override
  public List<PartitionGetParam> split() {
    if(partKeyToIndexesMap == null) {
      partKeyToIndexesMap = split(PSAgentContext.get().getMatrixMetaManager()
        .getPartitions(matrixId, rowId), indexes);
      indexes = null;
    }

    List<PartitionGetParam> partParams = new ArrayList<>(partKeyToIndexesMap.size());
    for(Map.Entry<PartitionKey, long[]> entry : partKeyToIndexesMap.entrySet()) {
      partParams.add(new LongIndexPartGetParam(matrixId, rowId, entry.getKey(), entry.getValue()));
    }
    return partParams;
  }

  /**
   * Find the used partition of the specifiex index array of this matrix this row
   * @param partKeys all partitions of this matrix and this row
   * @param indexes specified index array
   * @return the used partition of the specifiex index array of this matrix this row
   */
  private Map<PartitionKey, long[]> split(List<PartitionKey> partKeys, long[] indexes) {
    // Sort the parts by partitionId
    Arrays.sort(indexes);

    HashMap<PartitionKey, long[]> ret = new HashMap<>();

    // Sort partition keys use start column index
    Collections.sort(partKeys,
      (PartitionKey key1, PartitionKey key2) -> {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
    });

    int ii = 0;
    int keyIndex = 0;
    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indexes.length || keyIndex < partKeys.size()) {
      int length = 0;
      long endOffset = partKeys.get(keyIndex).getEndCol();
      while (ii < indexes.length && indexes[ii] < endOffset) {
        ii++;
        length++;
      }

      if(length != 0) {
        long [] split = new long[length];
        System.arraycopy(indexes, ii - length, split, 0, length);
        ret.put(partKeys.get(keyIndex), split);
      }
      keyIndex++;
    }
    return ret;
  }

  public int getRowId() {
    return rowId;
  }

  public int size() {
    if(indexes != null) {
      return indexes.length;
    } else {
      int counter = 0;
      for(long[] partIndexes : partKeyToIndexesMap.values()) {
        counter += partIndexes.length;
      }
      return counter;
    }
  }

  public Map<PartitionKey, long[]> getPartKeyToIndexesMap() {
    return partKeyToIndexesMap;
  }
}
