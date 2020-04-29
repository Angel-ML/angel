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


package com.tencent.angel.spark.ml.embedding.line;

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
    float [] cloneInputFeats = new float[inputFeats.length];
    System.arraycopy(inputFeats, 0, cloneInputFeats, 0, inputFeats.length);

    float [] cloneOutputFeats;
    if(outputFeats != null) {
      cloneOutputFeats = new float[outputFeats.length];
      System.arraycopy(outputFeats, 0, cloneOutputFeats, 0, outputFeats.length);
    }
    return new LINENode(inputFeats, outputFeats);
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(inputFeats.length);
    for(int i = 0; i < inputFeats.length; i++) {
      output.writeFloat(inputFeats[i]);
    }

    if (outputFeats != null) {
      output.writeInt(outputFeats.length);
      for(int i = 0; i < outputFeats.length; i++) {
        output.writeFloat(outputFeats[i]);
      }
    } else {
      output.writeInt(0);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    inputFeats = new float[input.readInt()];
    for(int i = 0; i < inputFeats.length; i++) {
      inputFeats[i] = input.readFloat();
    }

    int size = input.readInt();
    if(size > 0) {
      outputFeats = new float[size];
      for(int i = 0; i < outputFeats.length; i++) {
        outputFeats[i] = input.readFloat();
      }
    }
  }

  @Override
  public int bufferLen() {
    return 4 + inputFeats.length * 4 + 4 + (outputFeats != null ? outputFeats.length * 4 : 0);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    byte [] data = new byte[bufferLen()];
    int index = 0;
    output.writeInt(data.length);
    index = writeInt(data, inputFeats.length, index);
    for (float inputFeat : inputFeats) {
      index = writeFloat(data, inputFeat, index);
    }

    if (outputFeats != null) {
      index = writeInt(data, outputFeats.length, index);

      for (float outputFeat : outputFeats) {
        index = writeFloat(data, outputFeat, index);
      }
    } else {
      writeInt(data, 0, index);
    }
    output.write(data);
  }

  private int writeInt(byte[] data, int v, int index) {
    data[index] = (byte)((v >>> 24) & 0xFF);
    data[index + 1] = (byte)((v >>> 16) & 0xFF);
    data[index + 2] = (byte)((v >>> 8) & 0xFF);
    data[index + 3] = (byte)((v) & 0xFF);
    return index + 4;
  }

  private int writeFloat(byte[] data, float v, int index) {
    int iv = Float.floatToIntBits(v);
    return writeInt(data, iv, index);
  }

  private int readInt(byte[] data, int index) {
    int ch1 = data[index] & 255;
    int ch2 = data[index + 1] & 255;
    int ch3 = data[index + 2] & 255;
    int ch4 = data[index + 3] & 255;
    int c =  (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4);
    return c;
  }

  private float readFloat(byte[] data, int index) {
    return Float.intBitsToFloat(readInt(data, index));
  }


  @Override
  public void deserialize(DataInputStream input) throws IOException {
    byte [] data = new byte[input.readInt()];
    input.readFully(data);
    int index = 0;

    int inputFeatLen = readInt(data, index);
    inputFeats = new float[inputFeatLen];
    index += 4;

    for(int i = 0; i < inputFeatLen; i++) {
      inputFeats[i] = readFloat(data, index);
      index += 4;
    }

    int outputFeatLen = readInt(data, index);
    index += 4;

    if(outputFeatLen > 0) {
      outputFeats = new float[outputFeatLen];
      for(int i = 0; i < outputFeatLen; i++) {
        outputFeats[i] = readFloat(data, index);
        index += 4;
      }
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
