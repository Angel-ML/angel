package com.tencent.angel.spark.ml.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HyperLogLogPlusElement implements IElement {

  private HyperLogLogPlus readCounter;
  private HyperLogLogPlus writeCounter;
  private int p, sp;
  private long node;
  private int isActive;
  private Long closeness;


  public HyperLogLogPlusElement(long node, int p, int sp) {
    readCounter = new HyperLogLogPlus(p, sp);
    this.node = node;
    this.p = p;
    this.sp = sp;
    this.isActive = 1;
    this.closeness = 0L;
    long hashed = jenkins(node, System.currentTimeMillis());
    readCounter.offerHashed(hashed);
    writeCounter = readCounter;
  }

  private HyperLogLogPlusElement(long node, int p, int sp,
                                 HyperLogLogPlus readCounter, HyperLogLogPlus writeCounter,
                                 int isActive, long closeness) {
    this.node = node;
    this.p = p;
    this.sp = sp;
    this.readCounter = readCounter;
    this.writeCounter = writeCounter;
    this.isActive = isActive;
    this.closeness = closeness;
  }

  public HyperLogLogPlus getHyperLogLogPlus() {
    return readCounter;
  }

  public boolean isActive() { return isActive > 0; }

  public long getCloseness() { return closeness; }

  public void updateCloseness(long r) {
    long delta = writeCounter.cardinality() - readCounter.cardinality();
    if (delta > 0) {
      this.closeness += r * delta;
      readCounter = writeCounter;
    } else {
      this.isActive = 0;
    }
  }

  public void merge(HyperLogLogPlus other) {
    try {
      writeCounter = (HyperLogLogPlus) writeCounter.merge(other);
    } catch (CardinalityMergeException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeLong(node);
    output.writeInt(p);
    output.writeInt(sp);
    output.writeInt(isActive);
    output.writeLong(closeness);
    try {
      byte[] bytes = readCounter.getBytes();
      output.writeInt(bytes.length);
      output.writeBytes(bytes);
      bytes = writeCounter.getBytes();
      output.writeInt(bytes.length);
      output.writeBytes(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    node = input.readLong();
    p = input.readInt();
    sp = input.readInt();
    isActive = input.readInt();
    closeness = input.readLong();
    int len = input.readInt();
    byte[] bytes = new byte[len];
    input.readBytes(bytes);
    try {
      readCounter = HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    len = input.readInt();
    bytes = new byte[len];
    try {
      writeCounter = HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public int bufferLen() {
    return 8 + 4 + 4 + 4 + 8 + 4 + readCounter.sizeof() + 4 + writeCounter.sizeof();
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeLong(node);
    output.writeInt(p);
    output.writeInt(sp);
    output.writeInt(isActive);
    output.writeLong(closeness);
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
    closeness = input.readLong();
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
  public HyperLogLogPlusElement deepClone() {
    try {
      byte[] bytes = readCounter.getBytes();
      HyperLogLogPlus readPlus = HyperLogLogPlus.Builder.build(bytes);
      bytes = writeCounter.getBytes();
      HyperLogLogPlus writePlus = HyperLogLogPlus.Builder.build(bytes);
      return new HyperLogLogPlusElement(node, p, sp, readPlus, writePlus, isActive, closeness);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
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
    a -= b; a -= c; a ^= (c >>> 43);
    b -= c; b -= a; b ^= (a << 9);
    c -= a; c -= b; c ^= (b >>> 8);
    a -= b; a -= c; a ^= (c >>> 38);
    b -= c; b -= a; b ^= (a << 23);
    c -= a; c -= b; c ^= (b >>> 5);
    a -= b; a -= c; a ^= (c >>> 35);
    b -= c; b -= a; b ^= (a << 49);
    c -= a; c -= b; c ^= (b >>> 11);
    a -= b; a -= c; a ^= (c >>> 12);
    b -= c; b -= a; b ^= (a << 18);
    c -= a; c -= b; c ^= (b >>> 22);
    return c;
  }

}
