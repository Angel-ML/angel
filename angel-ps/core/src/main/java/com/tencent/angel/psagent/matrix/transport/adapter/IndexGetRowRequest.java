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

import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;

public abstract class IndexGetRowRequest extends UserRequest {
  private final int matrixId;
  private final int rowId;
  private final InitFunc func;

  public IndexGetRowRequest(int matrixId, int rowId, InitFunc func) {
    super(UserRequestType.INDEX_GET_ROW);
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.func = func;
  }

  public int getMatrixId() {
    return matrixId;
  }

  public int getRowId() {
    return rowId;
  }

  public InitFunc getFunc() {
    return func;
  }


  public abstract IndexType getIndexType();
}
