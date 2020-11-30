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


package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * Get matrix partition rpc request.
 */
public class GetPartitionRequest extends PartitionRequest {

  /**
   * Create a new GetPartitionRequest.
   *
   * @param partKey matrix partition key
   * @param clock   clock value
   */
  public GetPartitionRequest(PartitionKey partKey, int clock) {
    super(clock, partKey);
  }

  /**
   * Create a new GetPartitionRequest.
   */
  public GetPartitionRequest() {
    super();
  }

  @Override public TransportMethod getType() {
    return TransportMethod.GET_PART;
  }

  @Override public int getEstimizeDataSize() {
    MatrixMeta meta =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(partKey.getMatrixId());
    if (meta == null) {
      return 0;
    } else {
      RowType rowType = meta.getRowType();
      switch (rowType) {
        case T_DOUBLE_DENSE:
          return 8 * ((int) partKey.getEndCol() - (int) partKey.getStartCol() * (partKey.getEndRow()
            - partKey.getStartRow()));

        case T_INT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol() * (partKey.getEndRow()
            - partKey.getStartRow()));

        case T_FLOAT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol() * (partKey.getEndRow()
            - partKey.getStartRow()));

        default: {
          return 0;
        }
      }
    }
  }
}