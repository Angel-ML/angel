package com.tencent.angel.spark.ml.embedding.line2;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LINENode implements IElement {

  private float[] inputFeats;
  private float[] outputFeats;

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
    return 4 + inputFeats.length * 4 + 4 + (outputFeats != null ? outputFeats.length * 8 : 0);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
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
  public void deserialize(DataInputStream input) throws IOException {
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
  public int dataLen() {
    return bufferLen();
  }
}
