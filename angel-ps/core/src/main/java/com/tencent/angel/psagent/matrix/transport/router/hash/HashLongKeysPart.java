package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.collections.DynamicLongArray;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import io.netty.buffer.ByteBuf;

public class HashLongKeysPart extends HashKeyPart implements ILongKeyPartOp {
  /**
   * Dynamic long array, it is only used in client(before serialization)
   */
  private DynamicLongArray dynamicKeys;

  /**
   * Final long array, it is only used in server(after de-serialization)
   */
  private long[] keys;

  public HashLongKeysPart(int rowId, DynamicLongArray dynamicKeys) {
    super(rowId);
    this.dynamicKeys = dynamicKeys;
  }

  public HashLongKeysPart(int rowId, int estSize) {
    this(rowId, new DynamicLongArray(estSize));
  }


  public HashLongKeysPart(int estSize) {
    this(-1, new DynamicLongArray(estSize));
  }

  /**
   * Only use in deserialization
   */
  public HashLongKeysPart() {
    this(-1, new DynamicLongArray(1));
  }


  @Override
  public KeyType getKeyType() {
    return KeyType.LONG;
  }


  @Override
  public long[] getKeys() {
    // It is recommended not to call this method on the client
    if(keys == null) {
      keys = dynamicKeys.getData();
    }
    return keys;
  }

  @Override
  public void add(long[] keys) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i]);
    }
  }

  @Override
  public int size() {
    if(keys != null) {
      return keys.length;
    } else {
      return dynamicKeys.size();
    }
  }

  @Override
  public void add(long key) {
    dynamicKeys.add(key);
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeys.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    keys = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + dynamicKeys.bufferLen();
  }
}
