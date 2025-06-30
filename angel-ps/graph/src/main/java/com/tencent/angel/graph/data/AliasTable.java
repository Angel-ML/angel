package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (accept, alias)
 */
public class AliasTable implements IElement {

  private float[] accept;
  private int[] alias;


  public AliasTable() {
    this(null, null);
  }

  public AliasTable(float[] accept, int[] alias) {
    assert (accept.length == alias.length);
    this.alias = alias;
    this.accept = accept;
  }

  public float[] getAccept() {
    return accept;
  }

  public int[] getAlias() {
    return alias;
  }

  public int getNodesNum() {
    return alias.length;
  }

  public void setAlias(int[] newAlias) {
    alias = newAlias;
  }

  public void setAccept(float[] newAccept) {
    accept = newAccept;
  }

  @Override
  public Object deepClone() {
    int len = alias.length;
    int[] newAlias = new int[len];
    float[] newAccept = new float[len];

    System.arraycopy(accept, 0, newAccept, 0, len);
    System.arraycopy(alias, 0, newAlias, 0, len);
    return new AliasTable(newAccept, newAlias);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeFloats(output, accept);
    ByteBufSerdeUtils.serializeInts(output, alias);
  }

  @Override
  public void deserialize(ByteBuf input) {
    accept = ByteBufSerdeUtils.deserializeFloats(input);
    alias = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeFloats(output, accept);
    StreamSerdeUtils.serializeInts(output, alias);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    accept = StreamSerdeUtils.deserializeFloats(input);
    alias = StreamSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedFloatsLen(accept)
        + ByteBufSerdeUtils.serializedIntsLen(alias);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
  
}