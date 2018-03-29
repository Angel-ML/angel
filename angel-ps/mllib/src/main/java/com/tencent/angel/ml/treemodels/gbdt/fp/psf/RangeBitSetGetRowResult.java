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

import com.tencent.angel.ml.treemodels.gbdt.fp.RangeBitSet;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.psagent.matrix.ResponseType;
import io.netty.buffer.ByteBuf;

public class RangeBitSetGetRowResult extends GetResult {
  private RangeBitSet bitset;

  public RangeBitSetGetRowResult(ResponseType type, RangeBitSet bitset) {
    super(type);
    this.bitset = bitset;
  }

  public RangeBitSet getRangeBitSet() {
    return bitset;
  }

  public static class RangeBitSetPartitionGetRowResult extends PartitionGetResult {
    private RangeBitSet bitset;

    public RangeBitSetPartitionGetRowResult(RangeBitSet bitset) {
      this.bitset = bitset;
    }

    public RangeBitSetPartitionGetRowResult() {
      this.bitset = null;
    }

    public RangeBitSet getRangeBitSet() {
      return this.bitset;
    }

    /**
     * Serialize object to the Netty ByteBuf.
     *
     * @param buf the Netty ByteBuf
     */
    @Override public void serialize(ByteBuf buf) {
      this.bitset.serialize(buf);
    }

    /**
     * Deserialize object from the Netty ByteBuf.
     *
     * @param buf the Netty ByteBuf
     */
    @Override public void deserialize(ByteBuf buf) {
      if (buf.isReadable()) {
        this.bitset = new RangeBitSet();
        this.bitset.deserialize(buf);
      } else
        this.bitset = null;
    }

    /**
     * Estimate serialized data size of the object, it used to ByteBuf allocation.
     *
     * @return int serialized data size of the object
     */
    @Override public int bufferLen() {
      return this.bitset.bufferLen();
    }
  }
}
