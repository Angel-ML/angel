/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ps.storage.vector.storage;

import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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

  public void serialize(DataOutputStream output) throws IOException {
    output.writeLong(indexOffset);
  }

  public void deserialize(DataInputStream input) throws IOException {
    indexOffset = input.readLong();
  }

  public int dataLen() {
    return 8;
  }
}
