package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;

public class CompStreamKeyValuePart extends KeyValuePart {
  private KeyValuePart[] dataParts;
  private transient int position = 0;

  public CompStreamKeyValuePart(KeyValuePart[] dataParts) {
    super(-1);
    this.dataParts = dataParts;
  }

  public CompStreamKeyValuePart(int size) {
    this(new KeyValuePart[size]);
  }

  @Override
  public RowType getKeyValueType() {
    return subKeyValuePartType(dataParts);
  }

  @Override
  public RouterType getRouterType() {
    return subKeyValueRouterType(dataParts);
  }

  @Override
  public boolean isComp() {
    return true;
  }

  public CompStreamKeyValuePart() {
    this(null);
  }

  public void add(KeyValuePart subDataPart) {
    dataParts[position++] = subDataPart;
  }

  private RowType subKeyValuePartType(KeyValuePart[] dataParts) {
    if(dataParts == null || dataParts.length == 0) {
      return RowType.T_FLOAT_SPARSE;
    }

    for(int i = 0; i < dataParts.length; i++) {
      if(dataParts[i] != null) {
        return dataParts[i].getKeyValueType();
      }
    }

    return RowType.T_FLOAT_SPARSE;
  }

  private RouterType subKeyValueRouterType(KeyValuePart[] dataParts) {
    if(dataParts == null || dataParts.length == 0) {
      return RouterType.HASH;
    }

    for(int i = 0; i < dataParts.length; i++) {
      if(dataParts[i] != null) {
        return dataParts[i].getRouterType();
      }
    }

    return RouterType.HASH;
  }

  @Override
  public int size() {
    int count = 0;
    for(int i = 0; i < dataParts.length; i++) {
      if(dataParts[i] != null) {
        count += dataParts[i].size();
      }
    }

    return count;
  }

  @Override
  public void serialize(ByteBuf output) {
    // Need not call super.serialize
    // Valid sub data part number
    ByteBufSerdeUtils.serializeInt(output, validPartNum());
    for(int i = 0; i < dataParts.length; i++) {
      if(dataParts[i] != null) {
        ByteBufSerdeUtils.serializeKeyValuePart(output, dataParts[i]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    // Just use for stream model
    int size = ByteBufSerdeUtils.deserializeInt(input);
    dataParts = new KeyValuePart[size];
    for(int i = 0; i < size; i++) {
      dataParts[i] = ByteBufSerdeUtils.deserializeKeyValuePart(input);
    }
  }

  @Override
  public int bufferLen() {
    // Need not call super.serialize
    int len = ByteBufSerdeUtils.INT_LENGTH;
    for(int i = 0; i < dataParts.length; i++) {
      if(dataParts[i] != null) {
        len += dataParts[i].bufferLen();
      }
    }
    return len;
  }

  private int validPartNum() {
    int count = 0;
    for(int i = 0; i < dataParts.length; i++) {
      if(dataParts[i] != null) {
        count++;
      }
    }

    return count;
  }
}
