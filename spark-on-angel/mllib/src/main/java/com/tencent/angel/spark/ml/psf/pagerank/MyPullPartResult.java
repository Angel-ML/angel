package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;

import io.netty.buffer.ByteBuf;

public class MyPullPartResult extends PartitionGetResult {
  private long[] keys;
  private float[] vals;
  private long start;
  private FloatVector msgs, sums;
  private float resetProb, tol;

  public MyPullPartResult(long[] keys, long start,
                          FloatVector msgs, FloatVector sums,
                          float resetProb, float tol) {
    this.keys = keys;
    this.start = start;
    this.msgs = msgs;
    this.sums = sums;
    this.resetProb = resetProb;
    this.tol = tol;
  }

  public MyPullPartResult() {}

  public long getStart() {
    return start;
  }

  public long[] getKeys() {
    return keys;
  }

  public float[] getValues() {
    return vals;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeLong(start);
    int writerIndex = buf.writerIndex();
    buf.writeInt(0);
    int size;
    if (msgs instanceof IntFloatVector) {
      buf.writeByte(0); // int range
      size = serialize(buf, (IntFloatVector) msgs, (IntFloatVector) sums);
    } else {
      buf.writeByte(1); // long range
      size = serialize(buf, (LongFloatVector) msgs, (LongFloatVector) sums);
    }
    buf.setInt(writerIndex, size);
  }

  private int serialize(ByteBuf buf, IntFloatVector msgs, IntFloatVector sums) {
    int size = 0;
    for (int i = 0; i < keys.length; i++) {
      float val = msgs.get((int) keys[i]);
      if (val > 0) {
        buf.writeInt((int)keys[i]);
        buf.writeFloat(val);
        size++;
      }
    }
    return size;
  }

  private int serialize(ByteBuf buf, LongFloatVector msgs, LongFloatVector sums) {
    int size = 0;
    for (int i = 0; i < keys.length; i++) {
      float val = msgs.get(keys[i]);
      if (val > 0) {
        buf.writeLong(keys[i]);
        buf.writeFloat(val);
        size++;
      }
    }
    return size;
  }

  @Override
  public void deserialize(ByteBuf buf) {
    start = buf.readLong();
    int len = buf.readInt();
    keys = new long[len];
    vals = new float[len];
    byte type = buf.readByte();
    switch (type) {
      case 0: // int range
        for (int i = 0; i < len; i++) {
          keys[i] = buf.readInt() + start;
          vals[i] = buf.readFloat();
        }
        break;
      case 1: // long range
        for (int i = 0; i < len; i++) {
          keys[i] = buf.readLong() + start;
          vals[i] = buf.readFloat();
        }
    }
  }

  @Override
  public int bufferLen() {
    int len = 8 + 4 + 1;
    if (msgs instanceof IntFloatVector)
      len += ((IntFloatVector) msgs).size() * 8;
    else
      len += msgs.getSize() * 12;
    return len;
  }

}
