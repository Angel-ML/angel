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
/*
  Author: zhitaowang (zhitaowang@tecent.com)
  Created At: 2021/03/01
 */

package com.tencent.angel.spark.ml.graph.algo.embedding.bge;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A user-define data type that store node information on PS
 */
public class BGENode implements IElement {

  private float[] srcFeats;
  private float[] tgtFeats;

  // Use by bge with weight
  private transient int[] neighbors;
  private transient float[] weights;

  public BGENode(float[] srcFeats, float[] tgtFeats) {
    this.srcFeats = srcFeats;
    this.tgtFeats = tgtFeats;
  }

  public BGENode() {
    this(null, null);
  }

  public float[] getSrcFeats() {
    return srcFeats;
  }

  public void setSrcFeats(float[] srcFeats) {
    this.srcFeats = srcFeats;
  }

  public float[] getTgtFeats() {
    return tgtFeats;
  }

  public void setTgtFeats(float[] tgtFeats) {
    this.tgtFeats = tgtFeats;
  }

  public int[] getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(int[] neighbors) {
    this.neighbors = neighbors;
  }

  public float[] getWeights() {
    return weights;
  }

  public void setWeights(float[] weights) {
    this.weights = weights;
  }

  @Override
  public Object deepClone() {
    float [] cloneSrcFeats = new float[srcFeats.length];
    System.arraycopy(srcFeats, 0, cloneSrcFeats, 0, srcFeats.length);

    float [] cloneTgtFeats = null;
    if(tgtFeats != null) {
      cloneTgtFeats = new float[tgtFeats.length];
      System.arraycopy(tgtFeats, 0, cloneTgtFeats, 0, tgtFeats.length);
    }
    return new BGENode(cloneSrcFeats, cloneTgtFeats);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeFloats(output, srcFeats);
    if (tgtFeats != null) {
      ByteBufSerdeUtils.serializeFloats(output, tgtFeats);
    } else {
      ByteBufSerdeUtils.serializeEmptyFloats(output);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    srcFeats = ByteBufSerdeUtils.deserializeFloats(input);
    tgtFeats = ByteBufSerdeUtils.deserializeFloats(input);
    if (tgtFeats.length == 0) {
      tgtFeats = null;
    }
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedFloatsLen(srcFeats)
            + (tgtFeats != null ? ByteBufSerdeUtils.serializedFloatsLen(tgtFeats)
            : ByteBufSerdeUtils.serializedEmptyFloatsLen());
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeFloats(output, srcFeats);
    if (tgtFeats != null) {
      StreamSerdeUtils.serializeFloats(output, tgtFeats);
    } else {
      StreamSerdeUtils.serializeEmptyFloats(output);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    srcFeats = StreamSerdeUtils.deserializeFloats(input);
    tgtFeats = StreamSerdeUtils.deserializeFloats(input);
    if (tgtFeats.length == 0) {
      tgtFeats = null;
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
