package com.tencent.angel;

import java.io.Serializable;
import java.util.Arrays;

public class ByteArray implements Serializable {
  public final byte[] data;
  private final int hashCode;

  public ByteArray(byte[] data) {
    this.data = data;
    this.hashCode = Arrays.hashCode(data);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteArray byteArray = (ByteArray) o;
    return Arrays.equals(data, byteArray.data);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  public byte[] getData() {
    return data;
  }
}
