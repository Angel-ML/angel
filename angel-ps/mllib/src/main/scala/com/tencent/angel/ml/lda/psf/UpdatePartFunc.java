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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UpdatePartFunc extends UpdateFunc {
  private final static Log LOG = LogFactory.getLog(UpdatePartFunc.class);

  public UpdatePartFunc(UpdateParam param) {
    super(param);
  }

  public UpdatePartFunc() {
    super(null);
  }

  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    CSRPartUpdateParam param = (CSRPartUpdateParam) partParam;

    PartitionKey pkey = partParam.getPartKey();

    pkey = psContext.getMatrixMetaManager().getMatrixMeta(pkey.getMatrixId())
      .getPartitionMeta(pkey.getPartitionId()).getPartitionKey();

    try {
      while (param.buf.isReadable()) {
        int row = param.buf.readInt();
        int len = param.buf.readInt();

        ServerRow serverRow = psContext.getMatrixStorageManager().getRow(pkey, row);
        try {
          serverRow.startWrite();
          if (serverRow.isDense())
            updateDenseIntRow((ServerIntIntRow) serverRow, param.buf, len);
          if (serverRow.isSparse())
            updateSparseIntRow((ServerIntIntRow) serverRow, param.buf, len);
        } finally {
          serverRow.endWrite();
        }
      }
    } finally {
      param.buf.release();
    }
  }

  public void updateDenseIntRow(ServerIntIntRow row, ByteBuf buf, int len) {
    int[] values = ServerRowUtils.getVector(row).getStorage().getValues();
    for (int i = 0; i < len; i++) {
      int key = buf.readInt();
      int val = buf.readInt();
      values[key] += val;
    }

  }

  public void updateSparseIntRow(ServerIntIntRow row, ByteBuf buf, int len) {
    for (int i = 0; i < len; i++) {
      int key = buf.readInt();
      int val = buf.readInt();
      row.set(key, row.get(key) + val);
    }
  }
}
