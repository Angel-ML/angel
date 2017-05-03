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

package com.tencent.angel.ml.matrix.udf;


public class TestUDFUtils {

  public static void fillInt(byte[] bytes,int data) {
    byte[] base = int2Bytes(data);
    System.arraycopy(base, 0, bytes, 0, 4);

    int length = 4;
    int destPos = 4;
    base = bytes;
    while(true){
      System.arraycopy(base, 0, bytes, destPos, length);
      destPos = destPos * 2;
      length = length * 2;
      if (destPos + length > bytes.length) {
        length = bytes.length - destPos;
      }
      if (destPos > bytes.length) {
        break;
      }
    }
  }

  private static byte[] int2Bytes(int data) {
    byte[] base = new byte[4];
    for (int i = 0; i < 4; i++) {
      int shift = 3 - i;
      base[i] = (byte) (data >> 8 * shift);
    }
    return base;
  }

  public int[] bytes2Ints(byte[] bytes) {
    int[] ints = new int[bytes.length / 4];
    for (int i = 0; i < ints.length; i++) {
      ints[i] =bytes2Int(getBytes(bytes,i*4,4));
    }
    return ints;
  }

  public static byte[] getBytes(byte[] bytes,int pos,int length){
    byte [] res = new byte[length];
    for(int i=0;i<length;i++){
      res[i]= bytes[pos+i];
    }
    return res;
  }

  public static int bytes2Int(byte[] bytes) {
    int res = 0;
    for (int i = 0; i < 4; i++) {
      int shift = (bytes[i]& 0xFF) << (3 - i) * 8;
      res += shift;
    }
    return res;
  }

  public static int bytes2Int(byte[] bytes,int pos){
    return bytes2Int(getBytes(bytes,pos,4));
  }


}
