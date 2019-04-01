package com.tencent.angel.ps.storage.vector.storage;

import io.netty.buffer.ByteBuf;

public abstract class Storage implements IStorage {

  protected long indexOffset;

  public Storage(long indexOffset) {
    this.indexOffset = indexOffset;
  }

  public long getIndexOffset() {
    return indexOffset;
  }

  public void setIndexOffset(long indexOffset) {
    this.indexOffset = indexOffset;
  }


  public void serialize(ByteBuf buf) {
    buf.writeLong(indexOffset);
  }

  public void deserialize(ByteBuf buf) {
    indexOffset = buf.readLong();
  }

  public int bufferLen() {
    return 8;
  }
}
