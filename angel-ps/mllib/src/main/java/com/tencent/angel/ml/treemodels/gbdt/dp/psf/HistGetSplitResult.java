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

package com.tencent.angel.ml.treemodels.gbdt.dp.psf;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.regression.GradPair;
import com.tencent.angel.psagent.matrix.ResponseType;
import io.netty.buffer.ByteBuf;

public class HistGetSplitResult extends GetResult {
  private final SplitEntry splitEntry;

  public HistGetSplitResult(ResponseType type, SplitEntry splitEntry) {
    super(type);
    this.splitEntry = splitEntry;
  }

  public SplitEntry getSplitEntry() {
    return splitEntry;
  }

  public static class HistGetSplitPartitionResult extends PartitionGetResult {
    private SplitEntry splitEntry;

    public HistGetSplitPartitionResult(SplitEntry splitEntry) {
      this.splitEntry = splitEntry;
    }

    public HistGetSplitPartitionResult() {
      this(null);
    }

    public SplitEntry getSplitEntry() {
      return splitEntry;
    }

    @Override public void serialize(ByteBuf buf) {
      buf.writeInt(splitEntry.getFid());
      if (splitEntry.getFid() != -1) {
        buf.writeFloat(splitEntry.getFvalue());
        buf.writeFloat(splitEntry.getLossChg());
        GradPair leftGradPair = splitEntry.getLeftGradPair();
        buf.writeFloat(leftGradPair.getGrad());
        buf.writeFloat(leftGradPair.getHess());
        GradPair rightPair = splitEntry.getRightGradPair();
        buf.writeFloat(rightPair.getGrad());
        buf.writeFloat(rightPair.getHess());
      }
    }

    @Override public void deserialize(ByteBuf buf) {
      int fid = buf.readInt();
      if (fid != -1) {
        float fvalue = buf.readFloat();
        float lossChg = buf.readFloat();
        splitEntry = new SplitEntry(fid, fvalue, lossChg);
        float leftSumGrad = buf.readFloat();
        float leftSumHess = buf.readFloat();
        GradPair leftGradPair = new GradPair(leftSumGrad, leftSumHess);
        splitEntry.setLeftGradPair(leftGradPair);
        float rightSumGrad = buf.readFloat();
        float rightSumHess = buf.readFloat();
        GradPair rightGradPair = new GradPair(rightSumGrad, rightSumHess);
        splitEntry.setRightGradPair(rightGradPair);
      } else {
        splitEntry = new SplitEntry();
      }
    }

    @Override public int bufferLen() {
      return splitEntry.getFid() != -1 ? 28 : 4;
    }
  }
}
