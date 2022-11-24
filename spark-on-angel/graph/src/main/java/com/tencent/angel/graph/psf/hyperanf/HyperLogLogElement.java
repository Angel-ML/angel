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
package com.tencent.angel.graph.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HyperLogLogElement implements IElement {

  private static final Log LOG = LogFactory.getLog(UpdateHyperLogLogPartParam.class);

  private HyperLogLogPlus readCounter;
  private HyperLogLogPlus writeCounter;
  private int p, sp;
  private long node;
  private long seed;
  private int isActive;

  public HyperLogLogElement(long node, int p, int sp, long seed) {
    readCounter = new HyperLogLogPlus(p, sp);
    this.node = node;
    this.seed = seed;
    this.p = p;
    this.sp = sp;
    this.isActive = 1;
    long hashed = jenkins(node, seed);
    readCounter.offerHashed(hashed);
    writeCounter = readCounter;
  }

  public HyperLogLogElement(long node, int p, int sp, boolean edge) {
    readCounter = new HyperLogLogPlus(p, sp);
    this.node = node;
    this.p = p;
    this.sp = sp;
    this.isActive = 1;
    if (!edge) {
      long hashed = jenkins(node, System.currentTimeMillis());
      readCounter.offerHashed(hashed);
    }
    writeCounter = readCounter;
  }

  public HyperLogLogElement() { this(-1, -1, -1, null, null, -1, -1);}

  private HyperLogLogElement(long node, int p, int sp,
      HyperLogLogPlus readCounter, HyperLogLogPlus writeCounter,
      int isActive, long seed) {
    this.node = node;
    this.p = p;
    this.sp = sp;
    this.seed = seed;
    this.readCounter = readCounter;
    this.writeCounter = writeCounter;
    this.isActive = isActive;
  }

  public HyperLogLogPlus getHyperLogLogPlus() {
    return readCounter;
  }

  public HyperLogLogPlus getWriteCounter() {
    return writeCounter;
  }

  public long getCardDiff() {
    long delta = writeCounter.cardinality() - readCounter.cardinality();
    // readCounter = writeCounter;
    return delta;
  }

  public boolean isActive() { return isActive > 0; }

  public long getCardinality() {
    return readCounter.cardinality();
  }

  public void updateReadCounter() {
    long delta = writeCounter.cardinality() - readCounter.cardinality();
    this.isActive = delta > 0 ? 1 : 0;
    readCounter = writeCounter;
  }

  public void merge(HyperLogLogPlus other) {
    try {
      writeCounter = (HyperLogLogPlus) writeCounter.merge(other);
    } catch (CardinalityMergeException e) {
      LOG.error("Merge failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeLong(node);
    output.writeInt(p);
    output.writeInt(sp);
    output.writeInt(isActive);
    output.writeLong(seed);
    try {
      byte[] bytes = readCounter.getBytes();
      output.writeInt(bytes.length);
      output.writeBytes(bytes);
      bytes = writeCounter.getBytes();
      output.writeInt(bytes.length);
      output.writeBytes(bytes);
    } catch (IOException e) {
      LOG.error("Serialize failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    node = input.readLong();
    p = input.readInt();
    sp = input.readInt();
    isActive = input.readInt();
    seed = input.readLong();
    int len = input.readInt();
    byte[] bytes = new byte[len];
    input.readBytes(bytes);
    try {
      readCounter = HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      LOG.error("ReadCounter failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
    len = input.readInt();
    bytes = new byte[len];
    try {
      writeCounter = HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      LOG.error("WriteCounter failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.INT_LENGTH * 5 + ByteBufSerdeUtils.LONG_LENGTH * 2 + readCounter.sizeof() + writeCounter.sizeof();
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeLong(node);
    output.writeInt(p);
    output.writeInt(sp);
    output.writeInt(isActive);
    output.writeLong(seed);
    byte[] bytes = readCounter.getBytes();
    output.writeInt(bytes.length);
    output.writeBytes(new String(bytes));
    bytes = writeCounter.getBytes();
    output.writeInt(bytes.length);
    output.writeBytes(new String(bytes));
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    node = input.readLong();
    p = input.readInt();
    sp = input.readInt();
    isActive = input.readInt();
    seed = input.readLong();
    int len = input.readInt();
    byte[] bytes = new byte[len];
    input.readFully(bytes);
    readCounter = HyperLogLogPlus.Builder.build(bytes);
    len = input.readInt();
    bytes = new byte[len];
    input.readFully(bytes);
    writeCounter = HyperLogLogPlus.Builder.build(bytes);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  @Override
  public HyperLogLogElement deepClone() {
    try {
      byte[] bytes = readCounter.getBytes();
      HyperLogLogPlus readPlus = HyperLogLogPlus.Builder.build(bytes);
      bytes = writeCounter.getBytes();
      HyperLogLogPlus writePlus = HyperLogLogPlus.Builder.build(bytes);
      return new HyperLogLogElement(node, p, sp, readPlus, writePlus, isActive, seed);
    } catch (IOException e) {
      LOG.error("DeepClone failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
//    return null;
  }



  /**
   * Function to compute the hash function from node IDs.
   *
   * Taken from the WebGraph framework, specifically the class
   * IntHyperLogLogCounterArray.
   *
   * Note that the `x` parameter is a `Long`, but the function will also work
   * with `Int` values.
   *
   * @param x    the element to hash, i.e. the node ID
   * @param seed the seed to set up internal state.
   * @return the hashed value of `x`
   */
  private long jenkins(long x, long seed) {
    /* Set up the internal state */
    long a = seed + x;
    long b = seed;
    long c = 0x9e3779b97f4a7c13L; /* the golden ratio; an arbitrary value */
    a -= b;
    a -= c;
    a ^= (c >>> 43);
    b -= c;
    b -= a;
    b ^= (a << 9);
    c -= a;
    c -= b;
    c ^= (b >>> 8);
    a -= b;
    a -= c;
    a ^= (c >>> 38);
    b -= c;
    b -= a;
    b ^= (a << 23);
    c -= a;
    c -= b;
    c ^= (b >>> 5);
    a -= b;
    a -= c;
    a ^= (c >>> 35);
    b -= c;
    b -= a;
    b ^= (a << 49);
    c -= a;
    c -= b;
    c ^= (b >>> 11);
    a -= b;
    a -= c;
    a ^= (c >>> 12);
    b -= c;
    b -= a;
    b ^= (a << 18);
    c -= a;
    c -= b;
    c ^= (b >>> 22);
    return c;
  }

}

