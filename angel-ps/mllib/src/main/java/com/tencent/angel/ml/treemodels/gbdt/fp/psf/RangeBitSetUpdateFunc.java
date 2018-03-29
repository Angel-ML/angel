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
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ml.treemodels.gbdt.fp.RangeBitSet;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Modify bitset (presented by an array of Integers) on PS
 * The given byte array must be aligned to 8
 */
public class RangeBitSetUpdateFunc extends UpdateFunc {
  public static final Log LOG = LogFactory.getLog(RangeBitSetUpdateFunc.class);

  public RangeBitSetUpdateFunc(BitsUpdateParam param) {
    super(param);
  }

  public RangeBitSetUpdateFunc() {
    super(null);
  }

  public static class BitsUpdateParam extends UpdateParam {
    protected final RangeBitSet bitset;

    public BitsUpdateParam(int matrixId, boolean updateClock, RangeBitSet bitset) {
      super(matrixId, updateClock);
      this.bitset = bitset;
    }

    /**
     * Split list.
     *
     * @return the list
     */
    @Override public List<PartitionUpdateParam> split() {
      List<PartitionKey> partList =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();
      List<PartitionUpdateParam> partParamList = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        PartitionKey partKey = partList.get(i);
        int left = (int) partKey.getStartCol() * 32;
        int right = (int) partKey.getEndCol() * 32 - 1;
        RangeBitSet subset = this.bitset.overlap(left, right);
        if (subset == null)
          continue;
        BitsPartitionUpdateParam partParam =
          new BitsPartitionUpdateParam(matrixId, partKey, updateClock, subset, left * 8);
        partParamList.add(partParam);
      }
      return partParamList;
    }
  }


  public static class BitsPartitionUpdateParam extends PartitionUpdateParam {

    protected RangeBitSet bitset;
    protected int offset;

    public BitsPartitionUpdateParam(int matrixId, PartitionKey partKey, boolean updateClock,
      RangeBitSet bitset, int offset) {
      super(matrixId, partKey, updateClock);
      this.bitset = bitset;
      this.offset = offset;
    }

    public BitsPartitionUpdateParam() {
      super();
      bitset = null;
      offset = -1;
    }

    @Override public void serialize(ByteBuf buf) {
      super.serialize(buf);
      bitset.serialize(buf);
      buf.writeInt(offset);
    }

    @Override public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      if (buf.isReadable()) {
        bitset = new RangeBitSet();
        bitset.deserialize(buf);
        offset = buf.readInt();
      } else {
        bitset = null;
        offset = -1;
      }
    }

    @Override public int bufferLen() {
      return super.bufferLen() + bitset.bufferLen() + 4;
    }
  }

  /**
   * Partition update.
   *
   * @param partParam the partition parameter
   */
  @Override public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        if (row == null) {
          continue;
        }
        bitsUpdate(row, (BitsPartitionUpdateParam) partParam);
      }
    }
  }

  private void bitsUpdate(ServerRow row, BitsPartitionUpdateParam partParam) {
    switch (row.getRowType()) {
      case T_INT_DENSE:
        bitsUpdate((ServerDenseIntRow) row, partParam);
        break;
      default:
        break;
    }
  }

  private void bitsUpdate(ServerDenseIntRow row, BitsPartitionUpdateParam partParam) {
    if (partParam.bitset == null)
      return;
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      int from = partParam.bitset.getRangeFrom() - partParam.offset;
      int to = partParam.bitset.getRangeTo() - partParam.offset;
      LOG.debug(String.format("[%d-%d] ==> [%d-%d]", partParam.bitset.getRangeFrom(),
        partParam.bitset.getRangeTo(), from, to));
      byte[] bits = partParam.bitset.toByteArray();
      int first = from >> 3;
      int last = to >> 3;
      // first byte
      byte firstByte = 0;
      int t = from & 0b111;
      for (int i = 0; i < t; i++)
        firstByte |= data[first] & (1 << i);
      for (int i = t; i < 8; i++)
        firstByte |= bits[0] & (1 << i);
      data[first] = firstByte;
      // last byte
      byte lastByte = 0;
      t = to & 0b111;
      for (int i = 0; i <= t; i++)
        lastByte |= bits[last - first] & (1 << i);
      for (int i = t + 1; i < 8; i++)
        lastByte |= data[last] & (1 << i);
      data[last] = lastByte;
      // other bytes
      first++;
      last--;
      if (last >= first)
        System.arraycopy(bits, 1, data, first, last - first + 1);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}



