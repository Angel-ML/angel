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


package com.tencent.angel.graph.embedding.line;

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
public class LINENode implements IElement {

  private float[] inputFeats;
  private float[] outputFeats;

  // Use by line with weight
  private transient int[] neighbors;
  private transient float[] weights;

  public LINENode(float[] inputFeats, float[] outputFeats) {
    this.inputFeats = inputFeats;
    this.outputFeats = outputFeats;
  }

  public LINENode() {
    this(null, null);
  }

  public float[] getInputFeats() {
    return inputFeats;
  }

  public void setInputFeats(float[] inputFeats) {
    this.inputFeats = inputFeats;
  }

  public float[] getOutputFeats() {
    return outputFeats;
  }

  public void setOutputFeats(float[] outputFeats) {
    this.outputFeats = outputFeats;
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
    float[] cloneInputFeats = new float[inputFeats.length];
    System.arraycopy(inputFeats, 0, cloneInputFeats, 0, inputFeats.length);

    float[] cloneOutputFeats;
    if (outputFeats != null) {
      cloneOutputFeats = new float[outputFeats.length];
      System.arraycopy(outputFeats, 0, cloneOutputFeats, 0, outputFeats.length);
    }
    return new LINENode(inputFeats, outputFeats);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeFloats(output, inputFeats);
    if (outputFeats != null) {
      ByteBufSerdeUtils.serializeFloats(output, outputFeats);
    } else {
      ByteBufSerdeUtils.serializeEmptyFloats(output);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    inputFeats = ByteBufSerdeUtils.deserializeFloats(input);
    outputFeats = ByteBufSerdeUtils.deserializeFloats(input);
    if (outputFeats.length == 0) {
      outputFeats = null;
    }
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedFloatsLen(inputFeats)
        + (outputFeats != null ? ByteBufSerdeUtils.serializedFloatsLen(outputFeats)
        : ByteBufSerdeUtils.serializedEmptyFloatsLen());
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeFloats(output, inputFeats);
    if (outputFeats != null) {
      StreamSerdeUtils.serializeFloats(output, outputFeats);
    } else {
      StreamSerdeUtils.serializeEmptyFloats(output);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    inputFeats = StreamSerdeUtils.deserializeFloats(input);
    outputFeats = StreamSerdeUtils.deserializeFloats(input);
    if (outputFeats.length == 0) {
      outputFeats = null;
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
