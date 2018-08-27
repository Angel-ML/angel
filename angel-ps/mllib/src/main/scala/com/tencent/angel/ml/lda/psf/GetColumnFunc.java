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


package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.getrows.PartitionGetRowsParam;
import com.tencent.angel.ps.storage.MatrixStorageManager;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetColumnFunc extends GetFunc {

  private final static Log LOG = LogFactory.getLog(GetColumnFunc.class);

  public GetColumnFunc(GetParam param) {
    super(param);
  }

  public GetColumnFunc() {
    super(null);
  }

  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    if (partParam instanceof PartitionGetRowsParam) {
      PartitionGetRowsParam param = (PartitionGetRowsParam) partParam;
      PartitionKey pkey = param.getPartKey();
      pkey = psContext.getMatrixMetaManager().getMatrixMeta(pkey.getMatrixId())
        .getPartitionMeta(pkey.getPartitionId()).getPartitionKey();

      List<Integer> reqCols = param.getRowIndexes();
      int start = reqCols.get(0);
      int end = reqCols.get(1);

      MatrixStorageManager manager = psContext.getMatrixStorageManager();
      Map<Integer, Int2IntOpenHashMap> cks = new HashMap();


      for (int col = start; col < end; col++)
        cks.put(col, new Int2IntOpenHashMap());

      int rowOffset = pkey.getStartRow();
      int rowLength = pkey.getEndRow();
      for (int r = rowOffset; r < rowLength; r++) {
        ServerRow row = manager.getRow(pkey, r);

        if (row instanceof ServerIntIntRow) {
          for (int col = start; col < end; col++) {
            Int2IntOpenHashMap map = cks.get(col);
            int k = ((ServerIntIntRow) row).get(col);
            if (k > 0)
              map.put(row.getRowId(), k);
          }
        }
      }

      return new PartColumnResult(cks);
    } else {
      return null;
    }
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    List<PartColumnResult> columnResults = new ArrayList<>(partResults.size());
    for (PartitionGetResult r : partResults) {
      if (r instanceof PartColumnResult)
        columnResults.add((PartColumnResult) r);
      else
        LOG.error("r should be PartColumnResult but it is " + r.getClass());
    }

    // calculate columns
    PartColumnResult first = columnResults.get(0);

    for (int i = 1; i < columnResults.size(); i++) {
      first.merge(columnResults.get(i));
    }

    return new ColumnGetResult(first.cks);
  }
}
