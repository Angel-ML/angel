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

import com.google.common.primitives.Ints;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The GetParam of IndexGetFunc
 */
public class IndexGetParam extends GetParam {
  private int matId;
  private int rowId;
  private int[] indexs;

  /**
   * @param matId matrixID
   * @param rowId rowID
   * @param indexs specified index
   */
  public IndexGetParam(int matId, int rowId, int[] indexs) {
    super(matId);
    this.rowId = rowId;
    this.indexs = indexs;
  }

  /**
   * Find the used partition of the specifiex index array of this matrix this row
   * @return partition get param of specified index
   */
  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId, rowId);

    return usedParts(parts, indexs);
  }

  /**
   * Find the used partition of the specifiex index array of this matrix this row
   * @param parts all partitions of this matrix and this row
   * @param indexs specified index array
   * @return the used partition of the specifiex index array of this matrix this row
   */
  public List<PartitionGetParam> usedParts(List<PartitionKey> parts, int[] indexs) {
    class AscAgeComparator implements Comparator<PartitionKey> {

      @Override
      public int compare(PartitionKey p1, PartitionKey p2) {
        return p1.getPartitionId() - p2.getPartitionId();
      }
    }

    // Sort the parts by partitionId
    parts.sort(new AscAgeComparator());

    List<PartitionGetParam> usedParts = new ArrayList<PartitionGetParam>();
    int paramId = 0;
    int j = 0;
    for (int i = 0; i < parts.size() &&  j< indexs.length; ) {
      long startCol = parts.get(i).getStartCol();
      long endCol = parts.get(i).getEndCol();

      if ( (long) indexs[j] >= startCol && (long) indexs[j] < endCol) {
        List<Integer> ids = new ArrayList<>();

        while ((long) indexs[j] < endCol) {
          ids.add(indexs[j]);
          j++;
          if (j == indexs.length) break;
        }


        usedParts.add(new IndexPartGetParam(matrixId, rowId, parts.get(i), Ints.toArray
            (ids), paramId));
        paramId++;
        i++;

      } else {
        i++;
      }

    }

    return  usedParts;
  }

  public int getRowId() {
    return rowId;
  }
}
