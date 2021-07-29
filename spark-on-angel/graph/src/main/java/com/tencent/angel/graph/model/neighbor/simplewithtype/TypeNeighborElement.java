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


package com.tencent.angel.graph.model.neighbor.simplewithtype;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang.ArrayUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * The complex object that contains a integer array,, it can be stored in PS
 */
public class TypeNeighborElement implements IElement {

  private int selfType;
  private long[] data;
  private float[] accept;
  private int[] alias;
  private int[] type;
  private int[] indptr; // indicate the index of nbrs' type

  public TypeNeighborElement(int selfType, long[] data, int[] type, int[] indptr) {
    this.selfType = selfType;
    this.data = data;
    this.type = type;
    this.indptr = indptr;
    this.accept = null;
    this.alias = null;
  }

  public TypeNeighborElement(long[] data) {
    this.data = data;
    this.accept = null;
    this.alias = null;
  }

  public TypeNeighborElement(int selfType, long[] data, float[] accept, int[] alias, int[] type, int[] indptr) {
    this.selfType = selfType;
    this.data = data;
    this.type = type;
    this.indptr = indptr;
    this.accept = accept;
    this.alias = alias;
  }

  public TypeNeighborElement(long[] data, float[] accept, int[] alias) {
    this.data = data;
    this.accept = accept;
    this.alias = alias;
  }

  public TypeNeighborElement() {
    this(-1, null, null, null, null, null);
  }

  public long[] getData() {
    return data;
  }

  public int[] getTypes() {
    return type;
  }

  public int[] getIndptr() {
    return indptr;
  }

  public int getSelfType() {
    return selfType;
  }

  public void setData(long[] data) {
    this.data = data;
  }

  public void setTypes(int[] type) {
    this.type = type;
  }

  public void setIndptr(int[] indptr) {
    this.indptr = indptr;
  }

  public void setSelfType(int selfType) {
    this.selfType = selfType;
  }

  public long[] Sample(int sampleType, Random random, long key, int count) {
    if (accept == null)
      return sample(sampleType, random, key, count);
    else
      return sampleWithAlias(sampleType, random, key, count);
  }

  private long[] sample(int sampleType, Random random, long key, int count) {
    long[] re = new long[count];
    if (type == null) {
      for (int i = 0; i < count; i ++) {
        re[i] = sample(0, data.length, random);
      }
    } else {
      int typeIdx = ArrayUtils.indexOf(type, sampleType);
      if (typeIdx < 0) {
        Arrays.fill(re, key);
      } else {
        int min = indptr[typeIdx];
        int max = indptr[typeIdx + 1];
        for (int i = 0; i < count; i ++) {
          re[i] = sample(min, max, random);
        }
      }
    }
    return re;
  }

  private long[] sampleWithAlias(int sampleType, Random random, long key, int count) {
    long[] re = new long[count];
    if (type == null) {
      for (int i = 0; i < count; i ++) {
        re[i] = sampleWithAlias(0, data.length, random);
      }
    } else {
      int typeIdx = ArrayUtils.indexOf(type, sampleType);
      if (typeIdx < 0) {
        Arrays.fill(re, key);
      } else {
        int min = indptr[typeIdx];
        int max = indptr[typeIdx + 1];
        for (int i = 0; i < count; i ++) {
          re[i] = sampleWithAlias(min, max, random);
        }
      }
    }
    return re;
  }

  private long sampleWithAlias(int min, int max, Random random) {
    int idx = random.nextInt(max) % (max - min) + min;
    float acc = random.nextFloat();
    if (acc < accept[idx])
      return data[idx];
    else
      return data[alias[idx]+min];
  }

  private long sample(int min, int max, Random random) {
    int idx = random.nextInt(max) % (max - min) + min;
    return data[idx];
  }

  @Override
  public TypeNeighborElement deepClone() {
    long[] newData = new long[data.length];
    System.arraycopy(data, 0, newData, 0, data.length);
    if (type != null) {
      int[] newType = new int[type.length];
      int[] newIndptr = new int[indptr.length];
      System.arraycopy(type, 0, newType, 0, data.length);
      System.arraycopy(indptr, 0, newIndptr, 0, indptr.length);
      if (accept != null) {
        float[] newAccept = new float[accept.length];
        int[] newAlias = new int[alias.length];
        System.arraycopy(accept, 0, newAccept, 0, accept.length);
        System.arraycopy(alias, 0, newAlias, 0, alias.length);
        return new TypeNeighborElement(selfType, newData, newAccept, newAlias, newType, newIndptr);
      } else {
        return new TypeNeighborElement(selfType, newData, newType, newIndptr);
      }
    } else {
      if (accept != null) {
        float[] newAccept = new float[accept.length];
        int[] newAlias = new int[alias.length];
        System.arraycopy(accept, 0, newAccept, 0, accept.length);
        System.arraycopy(alias, 0, newAlias, 0, alias.length);
        return new TypeNeighborElement(newData, newAccept, newAlias);
      } else {
        return new TypeNeighborElement(newData);
      }
    }
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(selfType);
    ByteBufSerdeUtils.serializeLongs(output, data);
    if (type != null) {
      ByteBufSerdeUtils.serializeInts(output, type);
    } else {
      output.writeInt(0);
    }
    if (indptr != null) {
      ByteBufSerdeUtils.serializeInts(output, indptr);
    } else {
      output.writeInt(0);
    }
    if (accept != null) {
      ByteBufSerdeUtils.serializeFloats(output, accept);
    } else {
      output.writeInt(0);
    }
    if (alias != null) {
      ByteBufSerdeUtils.serializeInts(output, alias);
    } else {
      output.writeInt(0);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    selfType = input.readInt();
    data = ByteBufSerdeUtils.deserializeLongs(input);
    type = ByteBufSerdeUtils.deserializeInts(input);
    indptr = ByteBufSerdeUtils.deserializeInts(input);
    accept = ByteBufSerdeUtils.deserializeFloats(input);
    alias = ByteBufSerdeUtils.deserializeInts(input);
    if (type.length == 0) {
      type = null;
    }
    if (indptr.length == 0) {
      indptr = null;
    }
    if (accept.length == 0) {
      accept = null;
    }
    if (alias.length == 0) {
      alias = null;
    }
  }

  @Override
  public int bufferLen() {
    int len = 4 + 4 * 5 + data.length * 8;
    if (type != null) {
      len += type.length * 4;
    }
    if (indptr != null) {
      len += indptr.length * 4;
    }
    if (accept != null) {
      len += accept.length * 4;
    }
    if (alias != null) {
      len += alias.length * 4;
    }
    return len;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(selfType);
    StreamSerdeUtils.serializeLongs(output, data);
    if (type != null) {
      StreamSerdeUtils.serializeInts(output, type);
    } else {
      output.writeInt(0);
    }
    if (indptr != null) {
      StreamSerdeUtils.serializeInts(output, indptr);
    } else {
      output.writeInt(0);
    }
    if (accept != null) {
      StreamSerdeUtils.serializeFloats(output, accept);
    } else {
      output.writeInt(0);
    }
    if (alias != null) {
      StreamSerdeUtils.serializeInts(output, alias);
    } else {
      output.writeInt(0);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    selfType = input.readInt();
    data = StreamSerdeUtils.deserializeLongs(input);
    type = StreamSerdeUtils.deserializeInts(input);
    indptr = StreamSerdeUtils.deserializeInts(input);
    accept = StreamSerdeUtils.deserializeFloats(input);
    alias = StreamSerdeUtils.deserializeInts(input);
    if (type.length == 0) {
      type = null;
    }
    if (indptr.length == 0) {
      indptr = null;
    }
    if (accept.length == 0) {
      accept = null;
    }
    if (alias.length == 0) {
      alias = null;
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
