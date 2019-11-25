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
package com.tencent.angel.graph.client.node2vec.data;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Edge implements IElement {
  private long[] srcdst;
  private IntFloatVector feats;
  private int[] types;
  private float[] attrs;

  public Edge(long src, long dst, IntFloatVector feats, int[] types, float[] attrs) {
    srcdst = new long[2];
    srcdst[0] = src;
    srcdst[1] = dst;
    this.feats = feats;
    this.types = types;
    this.attrs = attrs;
  }

  public Edge(long src, long dst, float weight) {
    srcdst = new long[2];
    srcdst[0] = src;
    srcdst[1] = dst;
    this.attrs = new float[3];
    this.attrs[0] = weight;
  }

  public Edge(float weight) {
    this.attrs = new float[3];
    this.attrs[0] = weight;
  }

  public Edge(long src, long dst, int[] types, float[] attrs) {
    this(src, dst, null, types, attrs);
  }

  public void setSrc(long src) {
    if (srcdst == null) {
      srcdst = new long[2];
      srcdst[0] = src;
    } else {
      srcdst[0] = src;
    }
  }

  public long getSrc() {
    if (srcdst != null) {
      return srcdst[0];
    } else {
      return -1L;
    }
  }

  public void setDst(long dst) {
    if (srcdst == null) {
      srcdst = new long[2];
      srcdst[1] = dst;
    } else {
      srcdst[1] = dst;
    }
  }

  public long getDst() {
    if (srcdst != null) {
      return srcdst[1];
    } else {
      return -1L;
    }
  }

  public IntFloatVector getFeats() {
    return feats;
  }

  public void setFeats(IntFloatVector feats) {
    this.feats = feats;
  }

  public int[] getTypes() {
    return types;
  }

  public void setTypes(int[] types) {
    this.types = types;
  }

  public float getWeight() {
    return attrs[0];
  }

  public void setWeight(float attrW) {
    attrs[0] = attrW;
  }

  public float getJ() {
    return attrs[1];
  }

  public void setJ(float sttr_j) {
    attrs[1] = sttr_j;
  }

  public float getP() {
    return attrs[2];
  }

  public void setP(float sttr_p) {
    attrs[2] = sttr_p;
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
