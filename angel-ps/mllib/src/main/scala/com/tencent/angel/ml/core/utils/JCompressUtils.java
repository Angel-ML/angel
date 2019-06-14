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


package com.tencent.angel.ml.core.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JCompressUtils {

  private static final Log LOG = LogFactory.getLog(JCompressUtils.class);

  public static class Quantification {

    public static int serializeFloat(ByteBuf buf, float[] arr, int numBits) {
      return serializeFloat(buf, arr, 0, arr.length, numBits);
    }

    public static int serializeDouble(ByteBuf buf, double[] arr, int numBits) {
      return serializeDouble(buf, arr, 0, arr.length, numBits);
    }

    public static int serializeFloat(ByteBuf buf, float[] arr,
        int start, int end, int numBits) {
      long startTime = System.currentTimeMillis();
      if (numBits < 2 || numBits > 16 || (numBits & (numBits - 1)) != 0) {
        numBits = 8;
        LOG.error("Compression bits should be in {2,4,8,16}");
      }
      int len = end- start;
      buf.writeInt(len);
      buf.writeInt(numBits);
      float maxAbs = 0.0f;
      for (int i = start; i < end; i++) {
        if (Math.abs(arr[i]) > maxAbs) maxAbs =  Math.abs(arr[i]);
      }
      buf.writeFloat(maxAbs);
      int byteSum = 0;
      int maxPoint = (int) Math.pow(2, numBits - 1) - 1;
      int itemPerByte = 8 / numBits;
      int bytePerItem = numBits / 8;
      for (int i = start; i < end; ) {
        if (bytePerItem >= 1) {
          int point = quantify(arr[i], maxAbs, maxPoint);
          byte[] tmp = serializeInt(point, numBits / 8);
          buf.writeBytes(tmp);
          byteSum += bytePerItem;
          i++;
        } else {
          int[] tmpQ = new int[itemPerByte];
          for (int j = 0; j < itemPerByte; j++) {
            if (i + j >= end)
              tmpQ[j] = 0;
            else
              tmpQ[j] = quantify(arr[i+j], maxAbs, maxPoint);
          }
          byte tmp = serializeInt(tmpQ, itemPerByte);
          buf.writeByte(tmp);
          byteSum++;
          i += itemPerByte;
        }
      }
//      LOG.info("original float length: " + len + Arrays.toString(Arrays.copyOfRange(arr, start, end)));
      LOG.info(String.format("compress %d floats to %d bytes, max abs: %f, max point: %d, cost %d ms",
          len, byteSum, maxAbs, maxPoint, System.currentTimeMillis() - startTime));
      return arr.length;
    }

    public static int serializeDouble(ByteBuf buf, double[] arr,
        int start, int end, int numBits) {
      long startTime = System.currentTimeMillis();
      if (numBits < 2 || numBits > 32 || (numBits & (numBits - 1)) != 0) {
        numBits = 8;
        LOG.error("Compression bits should be in {2,4,8,16,32}");
      }
      int len = end - start;
      buf.writeInt(len);
      buf.writeInt(numBits);
      double maxAbs = 0.0;
      for (int i = start; i < end; i++) {
        if (Math.abs(arr[i]) > maxAbs) maxAbs =  Math.abs(arr[i]);
      }
      buf.writeDouble(maxAbs);
      int byteSum = 0;
      int maxPoint = (int) Math.pow(2, numBits - 1) - 1;
      int itemPerByte = 8 / numBits;
      int bytePerItem = numBits / 8;
      for (int i = start; i < end; ) {
        if (bytePerItem >= 1) {
          int point = quantify(arr[i], maxAbs, maxPoint);
          byte[] tmp = serializeInt(point, numBits / 8);
          buf.writeBytes(tmp);
          byteSum += bytePerItem;
          i++;
        } else {
          int[] tmpQ = new int[itemPerByte];
          for (int j = 0; j < itemPerByte; j++) {
            if (i + j >= end)
              tmpQ[j] = 0;
            else
              tmpQ[j] = quantify(arr[i+j], maxAbs, maxPoint);
          }
          byte tmp = serializeInt(tmpQ, itemPerByte);
          buf.writeByte(tmp);
          byteSum++;
          i += itemPerByte;
        }
      }
      //LOG.info("original double length: " + len + Arrays.toString(Arrays.copyOfRange(arr, start, end)));
      LOG.info(String.format("compress %d doubles to %d bytes, max abs: %f, max point: %d, cost %d ms",
          len, byteSum, maxAbs, maxPoint, System.currentTimeMillis() - startTime));
      return arr.length;
    }

    public static float[] deserializeFloat(ByteBuf buf) {
      long startTime = System.currentTimeMillis();
      int length = buf.readInt();
      int numBits = buf.readInt();
      float maxAbs = buf.readFloat();
      int maxPoint = (int) Math.pow(2, numBits - 1) - 1;
      int itemPerByte = 8 / numBits;
      int bytePerItem = numBits / 8;

      float[] arr = new float[length];
      for (int i = 0; i < length;) {
        if (bytePerItem >= 1) {
          byte[] itemBytes = new byte[bytePerItem];
          buf.readBytes(itemBytes);
          int point = deserializeInt(itemBytes);
          float item = maxAbs / maxPoint * point;
          arr[i] = item;
          i++;
        } else {
          byte b = buf.readByte();
          int[] points = deserializeInt(b, itemPerByte);
          for (int point : points) {
            if (i < length) {
              arr[i] = maxAbs / maxPoint * point;
              i++;
            }
          }
        }
      }
      //LOG.info("parsed float length: " + length + Arrays.toString(arr));
      LOG.info(String.format("parse %d floats, max abs: %f, max point: %d, cost %d ms",
          length, maxAbs, maxPoint, System.currentTimeMillis() - startTime));
      return arr;
    }

    public static double[] deserializeDouble(ByteBuf buf) {
      long startTime = System.currentTimeMillis();
      int length = buf.readInt();
      int numBits = buf.readInt();
      double maxAbs = buf.readDouble();
      int maxPoint = (int) Math.pow(2, numBits - 1) - 1;
      int itemPerByte = 8 / numBits;
      int bytePerItem = numBits / 8;

      double[] arr = new double[length];
      for (int i = 0; i < length;) {
        if (bytePerItem >= 1) {
          byte[] itemBytes = new byte[bytePerItem];
          buf.readBytes(itemBytes);
          int point = deserializeInt(itemBytes);
          double item = maxAbs / maxPoint * point;
          arr[i] = item;
          i++;
        } else {
          byte b = buf.readByte();
          int[] points = deserializeInt(b, itemPerByte);
          for (int point : points) {
            if (i < length) {
              arr[i] = maxAbs / maxPoint * point;
              i++;
            }
          }
        }
      }
      //LOG.info("parsed double length: " + length + Arrays.toString(arr));
      LOG.info(String.format("parse %d double, max abs: %f, max point: %d, cost %d ms",
          length, maxAbs, maxPoint, System.currentTimeMillis() - startTime));
      return arr;
    }

    private static int quantify(float item, float threshold, int maxPoint) {
      int point = (int) Math.floor(item / threshold * maxPoint);
      point += (point < maxPoint && Math.random() > 0.5) ? 1 : 0;
      return point;
    }

    private static int quantify(double item, double threshold, int maxPoint) {
      int point = (int) Math.floor(item / threshold * maxPoint);
      point += (point < maxPoint && Math.random() > 0.5) ? 1 : 0;
      return point;
    }
  }

  public static byte[] serializeInt(int value, int numBytes) {
    assert Math.pow(2, 8 * numBytes - 1) > value;
    byte[] rec = new byte[numBytes];
    boolean isNeg = false;
    if (value < 0) {
      value = -value;
      isNeg = true;
    }
    for (int i = 0; i < numBytes; i++) {
      rec[numBytes - i - 1] = (byte) value;
      value >>>= 8;
    }
    if (isNeg) {
      rec[0] |= 0x80;
    }
    return rec;
  }

  public static byte serializeInt(int[] values, int numItems) {
    assert values.length == numItems && numItems > 1;
    int numBits = 8 / numItems;
    byte rec = new Byte("00");
    int signMask = 0x80;
    int valueOffset = numBits == 2 ? 6 : 4;
    for (int i = 0; i < values.length; i++) {
      int value = values[i];
      if (value < 0) {
        value = -value;
        rec |= signMask;
      }
      assert Math.pow(2, numBits - 1) > value;
      rec |= value << valueOffset;
      signMask >>= numBits;
      valueOffset -= numBits;
    }
    return rec;
  }

  private static int deserializeInt(byte[] buf) {
    int rec = 0;
    boolean isNegative = (buf[0] & 0x80) == 0x80;
    buf[0] &= 0x7F;
    int base = 0;
    for (int i = buf.length - 1; i >= 0; i--) {
      int value = buf[i] & 0x0FF;
      rec += value << base;
      base += 8;
    }
    if (isNegative) {
      rec = -rec;
    }
    return rec;
  }

  private static int[] deserializeInt(byte b, int numItems) {
    int[] rec = new int[numItems];
    int numBits = 8 / numItems;
    int signMask = 0x80;
    int valueMask = numBits == 2 ? 0x01 : 0x07;
    int valueOffset = numBits == 2 ? 6 : 4;
    for (int i = 0; i < numItems; i++) {
      boolean isNeg = (b & signMask) == signMask;
      int value = (b >> valueOffset) & valueMask;
      if (isNeg)
        value = -value;
      rec[i] = value;
      signMask >>= numBits;
      valueOffset -= numBits;
    }
    return rec;
  }

  public static void main(String[] argv) {
    Random ran = new Random();
    int len = 102;
    int numBits = 4;
    double[] dArr = new double[len];
    float[] fArr = new float[len];
    for (int i = 0; i < dArr.length; i++) {
      dArr[i] = ran.nextDouble() - 0.5;
    }
    ByteBuf buf1 = Unpooled.buffer(1000);
    JCompressUtils.Quantification.serializeDouble(buf1, dArr, numBits);
    JCompressUtils.Quantification.deserializeDouble(buf1);


    for (int i = 0; i < dArr.length; i++) {
      fArr[i] = (float) dArr[i];
    }
    ByteBuf buf2 = Unpooled.buffer(1000);
    JCompressUtils.Quantification.serializeFloat(buf2, fArr, numBits);
    JCompressUtils.Quantification.deserializeFloat(buf2);
  }
}
