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
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * Get matrix partition rpc request.
 */
public class GetPartitionRequest extends PartitionRequest {

  /**
   * Create a new GetPartitionRequest.
   *
   * @param serverId parameter server id
   * @param partKey  matrix partition key
   * @param clock    clock value
   */
  public GetPartitionRequest(ParameterServerId serverId, PartitionKey partKey, int clock) {
    super(serverId, clock, partKey);
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
          ServerPartition part =
            PSAgentContext.get().getMatricesCache().getPartition(partKey.getMatrixId(), partKey);
          if (part != null) {
            return part.bufferLen();
          } else {
            return 0;
          }
        }
      }
    }
  }
}
