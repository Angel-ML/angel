package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.collections.DynamicObjectArray;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.IAnyKeyPartOp;
import io.netty.buffer.ByteBuf;

public class HashAnyKeysPart extends HashKeyPart implements IAnyKeyPartOp {
  /**
   * Dynamic object array, it is only used in client(before serialization)
   */
  private DynamicObjectArray dynamicKeys;

  /**
   * Final object array, it is only used in server(after de-serialization)
   */
  private IElement[] keys;

  public HashAnyKeysPart(int rowId, DynamicObjectArray dynamicKeys) {
    super(rowId);
    this.dynamicKeys = dynamicKeys;
    this.keys = keys;
  }

  public HashAnyKeysPart(int rowId, int estSize) {
    this(rowId, new DynamicObjectArray(estSize));
  }


  public HashAnyKeysPart(DynamicObjectArray dynamicKeys) {
    this(-1, dynamicKeys);
  }

  public HashAnyKeysPart(int estSize) {
    this(-1, new DynamicObjectArray(estSize));
  }

  @Override
  public KeyType getKeyType() {
    return KeyType.ANY;
  }

  /**
   * Only use in deserialization
   */
  public HashAnyKeysPart() {
    this(-1, new DynamicObjectArray(1));
  }

  @Override
  public IElement[] getKeys() {
    // It is recommended not to call this method on the client
    if(keys == null) {
      keys = dynamicKeys.getData();
    }
    return keys;
  }

  @Override
  public void add(IElement key) {
    dynamicKeys.add(key);
  }

  @Override
  public void add(IElement[] keys) {
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
    keys = ByteBufSerdeUtils.deserializeObjects(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + dynamicKeys.bufferLen();
  }
}
