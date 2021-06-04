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

package com.tencent.angel.ps.storage.partition.storage;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.partition.op.IServerRowsStorageOp;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import io.netty.buffer.ByteBuf;

/**
 * Base class for row-based matrix partition storage
 */
public abstract class ServerRowsStorage extends ServerPartitionStorage implements
    IServerRowsStorageOp {

  /**
   * Create new ServerRowsStorage
   *
   * @param rowIdOffset Row id offset
   */
  public ServerRowsStorage(int rowIdOffset) {
    super(rowIdOffset);
  }

  /**
   * Create new ServerRowsStorage
   */
  public ServerRowsStorage() {
    this(0);
  }

  /**
   * Init the server rows in the storage
   *
   * @param partKey partition key
   * @param rowType row type
   * @param estElemNum estimate element number
   * @param valueClass row element type
   */
  public void init(
      PartitionKey partKey, RowType rowType, long estElemNum,
      Class<? extends IElement> valueClass, RouterType routerType) {
    int rowStart = partKey.getStartRow();
    int rowEnd = partKey.getEndRow();
    long startCol = partKey.getStartCol();
    long endCol = partKey.getEndCol();

    if(estElemNum < 0) {
      estElemNum = 0;
    }

    for (int rowIndex = rowStart; rowIndex < rowEnd; rowIndex++) {
      ServerRow row = ServerRowFactory
          .createServerRow(rowIndex, rowType, startCol, endCol, (int)estElemNum, valueClass, routerType);
      row.init();
      putRow(rowIndex, row);
    }
  }

  @Override
  public void update(ByteBuf buf, UpdateOp op) {
    int rowNum = ByteBufSerdeUtils.deserializeInt(buf);
    int rowId;
    RowType rowType;

    for (int i = 0; i < rowNum; i++) {
      // Filter head
      ByteBufSerdeUtils.deserializeBoolean(buf);
      ByteBufSerdeUtils.deserializeInt(buf);

      rowType = RowType.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
      rowId = ByteBufSerdeUtils.deserializeInt(buf);

      ServerRow row = getRow(rowId);
      row.update(rowType, buf, op);
    }
  }
}
