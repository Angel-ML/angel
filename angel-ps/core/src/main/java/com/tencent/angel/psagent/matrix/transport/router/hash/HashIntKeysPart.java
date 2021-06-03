package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.collections.DynamicIntArray;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyPartOp;
import io.netty.buffer.ByteBuf;

/**
 * Int keys hash partition
 */
public class HashIntKeysPart extends HashKeyPart implements IIntKeyPartOp {

  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicIntArray dynamicKeys;

  /**
   * Final int array, it is only used in server(after de-serialization)
   */
  private int[] keys;

  public HashIntKeysPart(int rowId, DynamicIntArray dynamicKeys) {
    super(rowId);
    this.dynamicKeys = dynamicKeys;
  }

  public HashIntKeysPart(int rowId, int estSize) {
    this(rowId, new DynamicIntArray(estSize));
  }

  public HashIntKeysPart(DynamicIntArray dynamicKeys) {
    this(-1, dynamicKeys);
  }

  public HashIntKeysPart(int estSize) {
    this(-1, new DynamicIntArray(estSize));
  }

  /**
   * Only use in deserialization
   */
  public HashIntKeysPart() {
    this(-1, null);
  }

  @Override
  public KeyType getKeyType() {
    return KeyType.INT;
  }

  @Override
  public int[] getKeys() {
    // It is recommended not to call this method on the client
    if(keys == null) {
      keys = dynamicKeys.getData();
    }
    return keys;
  }

  @Override
  public void add(int key) {
    dynamicKeys.add(key);
  }

  @Override
  public void add(int[] keys) {
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
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeys.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    keys = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + dynamicKeys.bufferLen();
  }
}
