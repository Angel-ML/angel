package com.tencent.angel.spark.ml.psf.louvainDensity.InitCommunityInfor;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：community information (numberSelfEdge, size, weightDegree,selfWeight)
 */
public class CommunityInforElement implements IElement {

  private long numberSelfEdge;
  private long size;
  private float weightDegree;
  private float selfWeight;


  public CommunityInforElement() {
    this(0, 0, 0, 0);
  }


  public CommunityInforElement(long numberSelfEdge, long size, float weightDegree,
      float selfWeight) {
    this.numberSelfEdge = numberSelfEdge;
    this.size = size;
    this.weightDegree = weightDegree;
    this.selfWeight = selfWeight;
  }

  public long getNumberSelfEdge() {
    return numberSelfEdge;
  }

  public long getSize() {
    return size;
  }

  public float getWeightDegree() {
    return weightDegree;
  }

  public float getSelfWeight() {
    return selfWeight;
  }

  @Override
  public Object deepClone() {
    return new CommunityInforElement(numberSelfEdge, size, weightDegree, selfWeight);
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeLong(numberSelfEdge);
    output.writeLong(size);
    output.writeFloat(weightDegree);
    output.writeFloat(selfWeight);
  }

  @Override
  public void deserialize(ByteBuf input) {
    numberSelfEdge = input.readLong();
    size = input.readLong();
    weightDegree = input.readFloat();
    selfWeight = input.readFloat();
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeLong(numberSelfEdge);
    output.writeLong(size);
    output.writeFloat(weightDegree);
    output.writeFloat(selfWeight);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    numberSelfEdge = input.readLong();
    size = input.readLong();
    weightDegree = input.readFloat();
    selfWeight = input.readFloat();
  }

  @Override
  public int bufferLen() {
    return 2 * Long.BYTES + 2 * Float.BYTES;
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
