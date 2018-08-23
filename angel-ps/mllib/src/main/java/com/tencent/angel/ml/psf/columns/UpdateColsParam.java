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


package com.tencent.angel.ml.psf.columns;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.math2.utils.VectorUtils;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.*;

import static sun.misc.Version.println;


public class UpdateColsParam extends UpdateParam {

  int[] rows;
  private Vector cols;
  Map<Long, Vector> values;


  public UpdateColsParam(int matId, int[] rows, Vector cols, Map<Long, Vector> values) {
    super(matId);
    this.rows = rows;
    this.cols = cols;
    this.values = values;
  }

  @Override public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionUpdateParam> params = new ArrayList<>();
    int start = 0, end = 0;
    for (PartitionKey pkey : pkeys) {
      long startCol = pkey.getStartCol();
      long endCol = pkey.getEndCol();
      if (start < ((IntKeyVector) cols).getDim() && VectorUtils.getLong(cols, start) >= startCol) {
        while (end < ((IntKeyVector) cols).getDim() && VectorUtils.getLong(cols, end) < endCol)
          end++;
        long[] part = new long[end - start];
        if (cols instanceof IntIntVector) {
          ArrayCopy
            .copy(((IntIntVector) cols).getStorage().getValues(), start, part, 0, end - start);
        } else {
          System.arraycopy(((IntLongVector) cols).getStorage().getValues(), start, part, 0,
            end - start);
        }

        long firstKey = 0l;
        for (Map.Entry<Long, Vector> first : values.entrySet()) {
          firstKey = first.getKey();
          break;
        }

        if (values.get(firstKey) instanceof IntDoubleVector) {

          IntDoubleVector[] updates = new IntDoubleVector[part.length];
          for (int i = 0; i < part.length; i++)
            updates[i] = (IntDoubleVector) values.get(part[i]);
          params.add(new PartitionUpdateColsParam(matrixId, pkey, rows, part,
            VFactory.compIntDoubleVector(rows.length, updates, part.length)));
        } else if (values.get(firstKey) instanceof IntFloatVector) {

          IntFloatVector[] updates = new IntFloatVector[part.length];
          for (int i = 0; i < part.length; i++)
            updates[i] = (IntFloatVector) values.get(part[i]);
          params.add(new PartitionUpdateColsParam(matrixId, pkey, rows, part,
            VFactory.compIntFloatVector(rows.length, updates, part.length)));
        } else {
          throw new AngelException("Update data type should be float or double!");
        }
        start = end;
      }
    }
    return params;
  }
}
