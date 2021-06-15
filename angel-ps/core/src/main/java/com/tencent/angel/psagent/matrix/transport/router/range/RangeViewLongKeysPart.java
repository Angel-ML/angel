package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewLongKeysPart extends RangeKeyPart implements ILongKeyPartOp {
  private long [] keys;

  public RangeViewLongKeysPart(int rowId, long [] keys, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.keys = keys;
  }

  public RangeViewLongKeysPart(long [] keys, int startPos, int endPos) {
    this(-1, keys, startPos, endPos);
  }

  public RangeViewLongKeysPart() {
    this(-1, null, -1, -1);
  }

  @Override
  public KeyType getKeyType() {
    return KeyType.LONG;
  }

  @Override
  public void serialize(ByteBuf out) {
    super.serialize(out);
    ByteBufSerdeUtils.serializeLongs(out, keys, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf in) {
    super.deserialize(in);
    keys = ByteBufSerdeUtils.deserializeLongs(in);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedLongsLen(keys, startPos, endPos);
  }

  @Override
  public int size() {
    if(endPos == -1) {
      return keys.length;
    } else {
      return endPos - startPos;
    }
  }

  @Override
  public long[] getKeys() {
    return keys;
  }

  @Override
  public void add(long key) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(long[] keys) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }
}
