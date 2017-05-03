/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.codec;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FixPointDenseDoubleCodec implements DenseDoubleCodec{
  protected final static Log LOG = LogFactory.getLog(FixPointDenseDoubleCodec.class);
  
  private int itemPerDouble;

  public FixPointDenseDoubleCodec(int itemPerDouble){
    this.itemPerDouble = itemPerDouble;
  }
  
  public FixPointDenseDoubleCodec(){
    this(1);
  }
  
  @Override
  public void encode(ByteBuf out, double[] values, int startPos, int length) {
    long startTime = System.currentTimeMillis();
    if (length == 0) {
      out.writeInt(length);
      return;
    }
    
    int end = startPos + length;
    
    // write the max abs
    int size = length;
    size += 1; 
    out.writeInt(size);
    int bitPerItem = 8 * 8 / itemPerDouble;
    int maxPoint = (int) Math.pow(2, bitPerItem - 1) - 1;
    double maxAbs = 0.0;
    for (int i = startPos ; i < end; i++) {
      if (Math.abs(values[i]) > maxAbs) {
        maxAbs = Math.abs(values[i]);
      }
    }
    out.writeDouble(maxAbs);  
    
    int totalBytes = 0;
    
    for(int i = startPos ; i < end; i++){
      double value = values[i];
      int point = (int) Math.floor(Math.abs(value) / maxAbs * maxPoint);
      point += (point < maxPoint && Math.random() > 0.5) ? 1 : 0; // add Bernoulli random variable
      byte[] tmp = int2ByteArray(point, bitPerItem / 8);
      if (value < -1e-10) {
        tmp[0] |= 0x80;
      }
      out.writeBytes(tmp);
      totalBytes += bitPerItem / 8;
    }
    
    LOG.info(String.format("compress %d doubles from %d bytes to %d bytes, " +
            "bit per item: %d, max point: %d, max abs: %f, cost %d ms",
            end - startPos, (end - startPos) * 8, totalBytes + 8,
            bitPerItem, maxPoint, maxAbs, System.currentTimeMillis() - startTime));
  }
  
  private static byte[] int2ByteArray(int value, int size) {
    assert Math.pow(2, 8 * size - 1) > value;
    byte[] rec = new byte[size];
    for (int i = 0; i < size; i++) {
      rec[size - i - 1] = (byte) value;
      value >>>= 8;
    }
    return rec;
  }
  
  @SuppressWarnings("unused")
  private static String byte2hex(byte [] buffer){
    String h = "";

    for(int i = 0; i < buffer.length; i++){
      String temp = Integer.toHexString(buffer[i] & 0xFF);
      if(temp.length() == 1){
        temp = "0" + temp;
      }
      h = h + " " + temp;
    }

    return h.trim();
  }
  
  private static int byteArray2int(byte[] buffer){
    int rec = 0;
    boolean isNegative = (buffer[0] & 0x80) == 0x80;
    buffer[0] &= 0x7F;  // set the negative flag to 0

    int base = 0;
    for (int i = buffer.length - 1; i >= 0; i--) {
      byte value = buffer[i];
      value <<= base;
      base += 8;
      rec += value;
    }

    if (isNegative) {
      rec = ~rec;
    }

    return rec;
  }

  @Override
  public void decode(ByteBuf in, double[] data, int startPos, int length) {
    int bitPerItem = 8 * 8 / itemPerDouble;
    int size = in.readInt();

    LOG.debug("update double to double, size: " + size);
    if (size <= 0)
      return;

    double maxAbs = in.readDouble();
    int maxPoint = (int) Math.pow(2, bitPerItem - 1) - 1;

    for (int i = 0; i < size - 1; i++) {
      byte[] itemBytes = new byte[bitPerItem / 8];
      in.readBytes(itemBytes);
      data[startPos + i] = (double) byteArray2int(itemBytes) / (double) maxPoint * maxAbs;
    }

    LOG.info(String.format("parse compressed %d double data, max abs: %f, max point: %d", size - 1,
        maxAbs, maxPoint));
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(itemPerDouble);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    itemPerDouble = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return 4;
  }
}
