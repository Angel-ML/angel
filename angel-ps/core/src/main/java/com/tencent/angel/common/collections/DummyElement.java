package com.tencent.angel.common.collections;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DummyElement implements IElement {

  public DummyElement() {

  }

  @Override
  public Object deepClone() {
    return null;
  }

  @Override
  public void serialize(ByteBuf output) {

  }

  @Override
  public void deserialize(ByteBuf input) {

  }

  @Override
  public int bufferLen() {
    return 0;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {

  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {

  }

  @Override
  public int dataLen() {
    return 0;
  }
}
