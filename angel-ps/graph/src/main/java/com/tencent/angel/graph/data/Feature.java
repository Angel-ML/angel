package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * features contains two parts to adapt dense + sparse feature
 * 1. dense: dense feature
 * 2. fields: fieldIds for sparse feature
 * 3. values: values for sparse feature, optional
 * 4. ptrs: values and indexes position pointer, used in sequence feature
 * 5. keyType: keyType for the type of index in sparse feature
 * dense feature: dense
 * sparse feature: fieldIds, indexes, optional(values)
 * sequence feature: fieldIds, indexes, ptrs, optional(values)
 * combination feature, dense + sparse: dense, fieldIds, indexes, optional(values)
 * combination feature, dense + sparse + sequence: dense, fieldIds, indexes, ptrs, optional(values)
 * ...
 */
public class Feature implements IElement {
  float[] dense;
  short[] fields;
  float[] values;
  int[] ptrs;

  FeatureKeyType keyType;

  public Feature(float[] dense, short[] fields, float[] values, int[] ptrs, FeatureKeyType keyType) {
    this.dense = dense;
    this.fields = fields;
    this.values = values;
    this.ptrs = ptrs;
    this.keyType = keyType;
  }

  public Feature(float[] dense, short[] fields, float[] values, FeatureKeyType keyType) {
    this(dense, fields, values, null, keyType);
  }

  public Feature(float[] dense) {
   this(dense, null, null, FeatureKeyType.INT);
  }

  public Feature(short[] fields, float[] values, FeatureKeyType keyType) {
    this(null, fields, values, keyType);
  }

  public Feature(short[] fields, FeatureKeyType keyType) {
    this(null, fields, null, keyType);
  }

  public Feature() {
    this(null, null, null, FeatureKeyType.INT);
  }

  @Override
  public Object deepClone() {
    float[] denseClone = null;
    if (dense != null) {
      int len = dense.length;
      denseClone = new float[dense.length];
      System.arraycopy(dense, 0, denseClone, 0, len);
    }

    short[] fieldsClone = null;
    if (fields != null) {
      int len = fields.length;
      fieldsClone = new short[len];
      System.arraycopy(fields, 0, fieldsClone, 0, len);
    }

    float[] valuesClone = null;
    if (values != null) {
      int len = values.length;
      valuesClone = new float[values.length];
      System.arraycopy(values, 0, valuesClone, 0, len);
    }

    int[] ptrsClone = null;
    if (ptrs != null) {
      int len = ptrs.length;
      ptrsClone = new int[len];
      System.arraycopy(ptrs, 0, ptrsClone, 0, len);
    }

    return new Feature(denseClone, fieldsClone, valuesClone, ptrsClone, keyType);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeFloats(output, dense);
    ByteBufSerdeUtils.serializeShorts(output, fields);
    ByteBufSerdeUtils.serializeFloats(output, values);
    ByteBufSerdeUtils.serializeInts(output, ptrs);
  }

  @Override
  public void deserialize(ByteBuf input) {
    dense = ByteBufSerdeUtils.deserializeFloats(input);
    fields = ByteBufSerdeUtils.deserializeShorts(input);
    values = ByteBufSerdeUtils.deserializeFloats(input);
    ptrs = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    int len = ByteBufSerdeUtils.serializedFloatsLen(dense) +
            ByteBufSerdeUtils.serializedShortsLen(fields) +
            ByteBufSerdeUtils.serializedFloatsLen(values) +
            ByteBufSerdeUtils.serializedIntsLen(ptrs);
    return len;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeFloats(output, dense);
    StreamSerdeUtils.serializeShorts(output, fields);
    StreamSerdeUtils.serializeFloats(output, values);
    StreamSerdeUtils.serializeInts(output, ptrs);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    dense = StreamSerdeUtils.deserializeFloats(input);
    fields = StreamSerdeUtils.deserializeShorts(input);
    values = StreamSerdeUtils.deserializeFloats(input);
    ptrs = StreamSerdeUtils.deserializeInts(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public float[] getDense() {
    return dense;
  }

  public void setDense(float[] dense) {
    this.dense = dense;
  }

  public short[] getFields() {
    return fields;
  }

  public void setFields(short[] fields) {
    this.fields = fields;
  }

  public float[] getValues() {
    return values;
  }

  public void setValues(float[] values) {
    this.values = values;
  }

  public int[] getPtrs() {
    return ptrs;
  }

  public void setPtrs(int[] ptrs) {
    this.ptrs = ptrs;
  }

  public FeatureKeyType getKeyType() {
    return keyType;
  }

  public void setKeyType(FeatureKeyType keyType) {
    this.keyType = keyType;
  }
}
