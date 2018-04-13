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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.IntBuffer;

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
        int len = param.buf.readShort();

        ServerRow serverRow = psContext.getMatrixStorageManager().getRow(pkey, row);
        serverRow.tryToLockWrite();
        try {
          ServerDenseIntRow denseIntRow = (ServerDenseIntRow) serverRow;
          IntBuffer buffer = denseIntRow.getData();
          for (int i = 0; i < len; i++) {
            short key = param.buf.readShort();
            int val = param.buf.readInt();
            buffer.put(key, buffer.get(key) + val);
          }
        } finally {
          serverRow.unlockWrite();
        }
      }
    } finally {
      param.buf.release();
    }
  }
}
