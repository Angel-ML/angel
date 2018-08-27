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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ps.server.data.request.UpdateOp;

public class UpdateRowsRequest extends UserRequest {
  private final int matrixId;
  private final UpdateOp op;

  /**
   * Create a new UserRequest
   *
   * @param matrixId matrix id
   */
  public UpdateRowsRequest(int matrixId, UpdateOp op) {
    super(UserRequestType.UPDATE_ROWS);
    this.matrixId = matrixId;
    this.op = op;
  }

  public int getMatrixId() {
    return matrixId;
  }
}
