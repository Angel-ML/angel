package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyPartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewIntKeysPart extends RangeKeyPart implements IIntKeyPartOp {
  private int [] keys;

  public RangeViewIntKeysPart(int rowId, int [] keys, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.keys = keys;
  }

  public RangeViewIntKeysPart(int [] keys, int startPos, int endPos) {
    this(-1, keys, startPos, endPos);
  }

  public RangeViewIntKeysPart() {
    this(-1, null, -1, -1);
  }

  @Override
  public KeyType getKeyType() {
    return KeyType.INT;
  }

  @Override
  public void serialize(ByteBuf out) {
    super.serialize(out);
    ByteBufSerdeUtils.serializeInts(out, keys, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf in) {
    super.deserialize(in);
    keys = ByteBufSerdeUtils.deserializeInts(in);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedIntsLen(keys, startPos, endPos);
  }

  @Override
  public int size() {
    if(endPos != -1) {
      return endPos - startPos;
    } else {
      return keys.length;
    }
  }

  @Override
  public int[] getKeys() {
    return keys;
  }

  @Override
  public void add(int key) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(int[] keys) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }
}
