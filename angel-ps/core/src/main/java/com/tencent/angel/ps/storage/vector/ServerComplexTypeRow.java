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
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.ObjectTypeStorage;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

/**
 * Base class for basic type row
 */
public abstract class ServerComplexTypeRow extends ServerRow {

  /**
   * Value type class
   */
  protected Class<? extends IElement> valueType;

  public ServerComplexTypeRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
      long startCol, long endCol, long estElemNum, ObjectTypeStorage storage, RouterType routerType) {
    super(rowId, rowType, startCol, endCol, estElemNum, storage, routerType);
    this.valueType = valueType;
  }

  protected void initStorage() {
    storage = ServerRowStorageFactory
        .getComplexTypeStorage(valueType, rowType, startCol, endCol, estElemNum, useAdaptiveStorage,
            sparseToDenseFactor, routerType);
  }
}
