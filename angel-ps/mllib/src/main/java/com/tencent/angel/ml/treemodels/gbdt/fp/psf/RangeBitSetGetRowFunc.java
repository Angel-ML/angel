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
 *
 */

package com.tencent.angel.ml.treemodels.gbdt.fp.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowFunc;
import com.tencent.angel.ml.treemodels.gbdt.fp.RangeBitSet;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class RangeBitSetGetRowFunc extends GetRowFunc {
  public static final Log LOG = LogFactory.getLog(RangeBitSetGetRowFunc.class);

  public RangeBitSetGetRowFunc(RangeBitSetGetParam param) {
    super(param);
  }

  public RangeBitSetGetRowFunc(int matrixId, int from, int to) {
    this(new RangeBitSetGetParam(matrixId, from, to));
  }

  public RangeBitSetGetRowFunc() {
  }


  public static class RangeBitSetGetParam extends GetParam {
    private int from;
    private int to;

    public RangeBitSetGetParam(int matrixId, int from, int to) {
      super(matrixId);
      this.from = from;
      this.to = to;
      LOG.debug(String.format("Get range [%d-%d]", from, to));
    }

    @Override public List<PartitionGetParam> split() {
      List<PartitionKey> partList =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();

      List<PartitionGetParam> partParams = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        PartitionKey partKey = partList.get(i);
        assert partKey.getStartRow() + 1 == partKey.getEndRow();
        int left = (int) partKey.getStartCol() * 32;
        int right = (int) partKey.getEndCol() * 32 - 1;
        int newFrom = Math.max(left, from);
        int newTo = Math.min(right, to);
        if (newFrom > newTo)
          continue;
        RangeBitSetPartitionGetParam partParam =
          new RangeBitSetPartitionGetParam(matrixId, partKey, newFrom, newTo);
        partParams.add(partParam);
      }

      return partParams;
    }

  }


  public static class RangeBitSetPartitionGetParam extends PartitionGetParam {
    private int rowId;
    private int from;
    private int to;

    public RangeBitSetPartitionGetParam(int matrixId, PartitionKey partKey, int from, int to) {
      super(matrixId, partKey);
      this.rowId = partKey.getStartRow();
      this.from = from;
      this.to = to;
      LOG.debug(String.format("Partition subset [%d-%d]", from, to));
    }

    public RangeBitSetPartitionGetParam() {
      super(-1, null);
      this.rowId = -1;
      this.from = -1;
      this.to = -1;
    }

    @Override public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(from);
      buf.writeInt(to);
    }

    @Override public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      this.rowId = buf.readInt();
      this.from = buf.readInt();
      this.to = buf.readInt();
    }

    @Override public int bufferLen() {
      return super.bufferLen() + 12;
    }
  }

  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    RangeBitSetPartitionGetParam param = (RangeBitSetPartitionGetParam) partParam;
    LOG.debug(String.format("Getting range [%d-%d]", param.from, param.to));

    ServerDenseIntRow row = (ServerDenseIntRow) psContext.getMatrixStorageManager()
      .getRow(param.getMatrixId(), param.rowId, param.getPartKey().getPartitionId());
    // get subset, in byte array
    int start = (int) (row.getStartCol() * 32);
    int end = (int) (row.getEndCol() * 32 - 1);
    byte[] data = row.getDataArray();
    RangeBitSet whole = new RangeBitSet(start, end, data);
    RangeBitSet subset = whole.subset(param.from, param.to);
    return new RangeBitSetGetRowResult.RangeBitSetPartitionGetRowResult(subset);
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    int size = partResults.size();
    List<RangeBitSetGetRowResult.RangeBitSetPartitionGetRowResult> partResultList =
      new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      partResultList
        .add((RangeBitSetGetRowResult.RangeBitSetPartitionGetRowResult) partResults.get(i));
    }
    if (size == 1) {
      RangeBitSet bitset = partResultList.get(0).getRangeBitSet();
      return new RangeBitSetGetRowResult(ResponseType.SUCCESS, bitset);
    } else {
      List<RangeBitSet> bitsets = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        bitsets.add(partResultList.get(i).getRangeBitSet());
      }
      RangeBitSet res = RangeBitSet.or(bitsets);
      return new RangeBitSetGetRowResult(ResponseType.SUCCESS, res);
    }
  }
}
