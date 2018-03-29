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

package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.multi.PartitionGetRowsParam;
import com.tencent.angel.ps.impl.MatrixStorageManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;


public class GetPartFunc extends GetFunc {

  private final static Log LOG = LogFactory.getLog(GetPartFunc.class);

  public GetPartFunc(GetParam param) {
    super(param);
  }

  public GetPartFunc() {
    super(null);
  }

  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    if (partParam instanceof PartitionGetRowsParam) {
      PartitionGetRowsParam param = (PartitionGetRowsParam) partParam;

      PartitionKey pkey = param.getPartKey();
      pkey = psContext.getMatrixMetaManager().getMatrixMeta(pkey.getMatrixId())
        .getPartitionMeta(pkey.getPartitionId()).getPartitionKey();
      int ws = pkey.getStartRow();
      int es = pkey.getEndRow();

      List<Integer> reqRows = param.getRowIndexes();

      MatrixStorageManager manager = psContext.getMatrixStorageManager();
      List<ServerRow> rows = new ArrayList<>();
      for (int w : reqRows)
        rows.add(manager.getRow(pkey, w));

      PartCSRResult csr = new PartCSRResult(rows);
      return csr;
    } else {
      return null;
    }
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    return null;
  }
}
