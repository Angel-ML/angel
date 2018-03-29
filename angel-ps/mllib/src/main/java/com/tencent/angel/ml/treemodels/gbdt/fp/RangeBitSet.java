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

package com.tencent.angel.ml.treemodels.gbdt.fp;

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Efficient bit set for a given range [from, to]
 * The bits will be aligned to 8 automatically
 */
public class RangeBitSet implements Serialize {
  private static final Log LOG = LogFactory.getLog(RangeBitSet.class);

  private byte[] bits;
  private int from;
  private int to;
  private int offset;

  public RangeBitSet(int from, int to) {
    LOG.debug(
      String.format("New RangeBitSet: [%d-%d], numBytes[%d]", from, to, needNumBytes(from, to)));
    this.from = from;
    this.to = to;
    this.offset = from & 0b111;
    this.bits = new byte[needNumBytes(from, to)];
  }

  public RangeBitSet(int from, int to, byte[] bits) {
    this.from = from;
    this.to = to;
    this.offset = from & 0b111;
    if (bits.length != needNumBytes(from, to)) {
      LOG.error(String
        .format("Invalid RangeBitSet size: %d, should be %d", bits.length, needNumBytes(from, to)));
    } else
      this.bits = bits;
  }

  public RangeBitSet() {
    this.from = -1;
    this.to = -1;
    this.offset = -1;
    this.bits = null;
  }

  private int needNumBytes(int from, int to) {
    int first = (int) (from >> 3);
    int last = (int) (to >> 3);
    return last - first + 1;
  }

  public void set(int index) {
    index = index - from + offset;
    int x = index >> 3;
    int y = index & 0b111;
    bits[x] = (byte) (bits[x] | (1 << y));
  }

  public void clear(int index) {
    index = index - from + offset;
    int x = index >> 3;
    int y = index & 0b111;
    bits[x] = (byte) (bits[x] & (~(1 << y)));
  }

  // TODO: use arraycopy to make it faster
  public void or(RangeBitSet other) {
    int from = other.getRangeFrom(), to = other.getRangeTo();
    assert from >= this.from && to <= this.to;
    for (int i = from; i <= to; i++) {
      if (other.get(i))
        set(i);
    }
  }

  public boolean get(int index) {
    index = index - from + offset;
    int x = index >> 3;
    int y = index & 0b111;
    return ((bits[x] >> y) & 0x1) == 1;
  }

  public RangeBitSet subset(int newFrom, int newTo) {
    if ((newFrom <= from && newTo >= to) || newFrom > newTo) {
      LOG.error(String
        .format("Invalid subset range: [%d-%d], should be in [%d-%d]", newFrom, newTo, from, to));
      return null;
    }
    LOG.debug(String.format("Create subset: [%d-%d]", newFrom, newTo));
    int firstByteIdx = (newFrom - from) >> 3;
    int lastByteIdx = (newTo - from) >> 3;
    int numBytes = lastByteIdx - firstByteIdx + 1;
    byte[] subset = new byte[numBytes];
    System.arraycopy(bits, firstByteIdx, subset, 0, numBytes);
    return new RangeBitSet(newFrom, newTo, subset);
  }

  public RangeBitSet overlap(int newFrom, int newTo) {
    LOG.debug(String.format("Get overlap: [%d-%d]", newFrom, newTo));
    newFrom = Math.max(newFrom, from);
    newTo = Math.min(newTo, to);
    if (newFrom > newTo)
      return null;
    if (newFrom != from || newTo != to) {
      return subset(newFrom, newTo);
    } else {
      return this;
    }
  }

  public byte[] toByteArray() {
    //return bits.clone();
    return bits;
  }

  public int getRangeFrom() {
    return from;
  }

  public int getRangeTo() {
    return to;
  }

  public int getNumValid() {
    int res = 0;
    for (int i = from; i <= to; i++) {
      if (get(i))
        res++;
    }
    return res;
  }

  @Override public void serialize(ByteBuf buf) {
    buf.writeInt(from);
    buf.writeInt(to);
    buf.writeInt(offset);
    buf.writeBytes(bits);
  }

  @Override public void deserialize(ByteBuf buf) {
    from = buf.readInt();
    to = buf.readInt();
    offset = buf.readInt();
    int numBytes = needNumBytes(from, to);
    bits = new byte[numBytes];
    buf.readBytes(bits);
  }

  @Override public int bufferLen() {
    return 12 + bits.length;
  }

  public static RangeBitSet or(RangeBitSet bs1, RangeBitSet bs2) {
    int from = Math.min(bs1.getRangeFrom(), bs2.getRangeFrom());
    int to = Math.max(bs1.getRangeTo(), bs2.getRangeTo());
    RangeBitSet res = new RangeBitSet(from, to);
    res.or(bs1);
    res.or(bs2);
    return res;
  }

  public static RangeBitSet or(List<RangeBitSet> bitsets) {
    int from = Integer.MAX_VALUE, to = Integer.MIN_VALUE;
    int size = bitsets.size();
    for (int i = 0; i < size; i++) {
      from = Math.min(bitsets.get(i).getRangeFrom(), from);
      to = Math.max(bitsets.get(i).getRangeTo(), to);
    }
    RangeBitSet res = new RangeBitSet(from, to);
    for (int i = 0; i < size; i++) {
      res.or(bitsets.get(i));
    }
    return res;
  }

}
