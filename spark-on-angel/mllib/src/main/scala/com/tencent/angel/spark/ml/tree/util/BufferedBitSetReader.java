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

package com.tencent.angel.spark.ml.tree.util;

public class BufferedBitSetReader {

  //private final ByteBuffer bytes;
  private final byte[] bytes;
  private int readIndexH;
  private int readMaskH;
  private int readBufferH;
  private int readIndexT;
  private int readMaskT;
  private int readBufferT;

  public BufferedBitSetReader(byte[] bytes, int numBits) {
    //public BufferedBitSetReader(ByteBuffer bytes, int numBits) {
    this.bytes = bytes;
    //int capacity = bytes.capacity() * 8;
    //readIndexT = bytes.capacity() - 1;
    int capacity = bytes.length * 8;
    this.readIndexT = bytes.length - 1;
    for (int i = numBits; i < capacity; i++) {
      readTail();
    }
  }

  public boolean readHead() {
    if (readMaskH == 0) {
      readBufferH = readFromBuffer(readIndexH++);
      readMaskH = 0b10000000;
    }
    boolean bit = (readBufferH & readMaskH) > 0;
    readMaskH >>= 1;
    return bit;
  }

  public boolean readTail() {
    if (readMaskT == 0) {
      readBufferT = readFromBuffer(readIndexT--);
      readMaskT = 0b00000001;
    }
    boolean bit = (readBufferT & readMaskT) > 0;
    readMaskT = (readMaskT << 1) & 0b11111111;
    return bit;
  }

  public boolean read(int index) {
    int x = index >> 3;
    int y = index & 0b111;
    byte b = readFromBuffer(x);
    return ((b >> (7 - y)) & 0x1) == 1;
  }

  public byte readFromBuffer(int index) {
    //return bytes.get(index);
    return bytes[index];
  }
}
