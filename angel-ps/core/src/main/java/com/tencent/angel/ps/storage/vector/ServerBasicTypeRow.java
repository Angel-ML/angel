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

package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.op.BasicTypePipelineOp;
import com.tencent.angel.ps.storage.vector.storage.BasicTypeStorage;

/**
 * Base class for basic type row
 */
public abstract class ServerBasicTypeRow extends ServerRow implements BasicTypePipelineOp {

  public ServerBasicTypeRow(int rowId, RowType rowType,
      long startCol, long endCol, long estElemNum,
      BasicTypeStorage storage) {
    super(rowId, rowType, startCol, endCol, estElemNum, storage);
  }

  protected void initStorage() {
    storage = ServerRowStorageFactory
        .getBasicTypeStorage(rowType, startCol, endCol, estElemNum, useAdaptiveKey, useAdaptiveStorage,
            sparseToDenseFactor);
  }
}
