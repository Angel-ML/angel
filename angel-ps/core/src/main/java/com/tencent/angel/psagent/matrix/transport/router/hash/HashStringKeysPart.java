package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.collections.DynamicStringArray;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IStringKeyPartOp;
import io.netty.buffer.ByteBuf;

public class HashStringKeysPart extends HashKeyPart implements IStringKeyPartOp {
  /**
   * Dynamic String array, it is only used in client(before serialization)
   */
  private DynamicStringArray dynamicKeys;

  /**
   * Final String array, it is only used in server(after de-serialization)
   */
  private String[] keys;

  public HashStringKeysPart(int rowId, DynamicStringArray dynamicKeys) {
    super(rowId);
    this.dynamicKeys = dynamicKeys;
  }

  public HashStringKeysPart(DynamicStringArray dynamicKeys) {
    this(-1, dynamicKeys);
  }

  public HashStringKeysPart(int rowId, int estSize) {
    this(rowId, new DynamicStringArray(estSize));
  }

  public HashStringKeysPart(int estSize) {
    this(-1, new DynamicStringArray(estSize));
  }

  /**
   * Only use in deserialization
   */
  public HashStringKeysPart() {
    this(-1, new DynamicStringArray(1));
  }

  @Override
  public KeyType getKeyType() {
    return KeyType.STRING;
  }

  @Override
  public String[] getKeys() {
    // It is recommended not to call this method on the client
    if(keys == null) {
      keys = dynamicKeys.getData();
    }
    return keys;
  }

  @Override
  public void add(String[] keys) {
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
  public void add(String key) {
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
    keys = ByteBufSerdeUtils.deserializeUTF8s(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + dynamicKeys.bufferLen();
  }
}
