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
package com.tencent.angel.ps.storage.partition;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.partition.op.ICSRStorageOp;
import com.tencent.angel.ps.storage.partition.storage.CSRStorage;
import io.netty.buffer.ByteBuf;

public class CSRPartition extends ServerPartition implements ICSRStorageOp {
  /**
   * Create new CSRPartition
   *
   * @param partKey partition key
   * @param storage row-based matrix partition storage
   * @param rowType row type
   */
  public CSRPartition(PartitionKey partKey, long estElemNum, CSRStorage storage,
      RowType rowType) {
    super(partKey, rowType, estElemNum, storage);
  }

  public CSRPartition() {
    this(null, -1, null, RowType.T_DOUBLE_DENSE);
  }

  @Override
  public CSRStorage getStorage() {
    return (CSRStorage) super.getStorage();
  }

  @Override
  public void update(ByteBuf buf, UpdateOp op) {
    throw new UnsupportedOperationException("pipeline update not support for CSRPartition now");
  }

  @Override
  public void init() {
    getStorage().init(partKey);
  }

  @Override
  public void reset() {
    getStorage().reset();
  }
}
