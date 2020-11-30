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


package com.tencent.angel.ml.matrix.psf.update.update;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitFactory;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * Partition increment parameter
 */
public class PartIncrementRowsParam extends PartitionUpdateParam {

  /**
   * PS increment splits
   */
  private List<RowUpdateSplit> updates;

  /**
   * Create PartIncrementRowsParam
   *
   * @param matrixId matrix id
   * @param part partition key
   * @param updates increment splits in the partition
   */
  public PartIncrementRowsParam(int matrixId, PartitionKey part, List<RowUpdateSplit> updates) {
    super(matrixId, part);
    this.updates = updates;
  }

  /**
   * Create a empty PartIncrementRowsParam
   */
  public PartIncrementRowsParam() {
    this(-1, null, null);
  }

  /**
   * Get increment splits
   *
   * @return increment splits
   */
  public List<RowUpdateSplit> getUpdates() {
    return updates;
  }

  /**
   * Set increment splits
   *
   * @param updates increment splits
   */
  public void setUpdates(
      List<RowUpdateSplit> updates) {
    this.updates = updates;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int size = updates.size();
    buf.writeInt(size);

    RowUpdateSplitContext context = new RowUpdateSplitContext();
    context.setEnableFilter(false);
    context.setPartKey(getPartKey());

    for (int i = 0; i < size; i++) {
      buf.writeInt(updates.get(i).getRowType().getNumber());
      updates.get(i).setSplitContext(context);
      updates.get(i).serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = buf.readInt();
    updates = new ArrayList<>(size);

    RowUpdateSplitContext context = new RowUpdateSplitContext();
    context.setPartKey(getPartKey());
    for (int i = 0; i < size; i++) {
      RowUpdateSplit split = RowUpdateSplitFactory.get(RowType.valueOf(buf.readInt()));
      split.setSplitContext(context);
      split.deserialize(buf);
      updates.add(split);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    int size = updates.size();
    for (int i = 0; i < size; i++) {
      len += 4;
      len += updates.get(i).bufferLen();
    }
    return len;
  }
}
