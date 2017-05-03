package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class V3FUpdaterParam extends UpdaterParam {

  public static class V3FPartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId1;
    private int rowId2;
    private int rowId3;
    private Serialize func;

    public V3FPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int rowId1, int rowId2, int rowId3, Serialize func) {
      super(matrixId, partKey, false);
      this.rowId1 = rowId1;
      this.rowId2 = rowId2;
      this.rowId3 = rowId3;
      this.func = func;
    }

    public V3FPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId1);
      buf.writeInt(rowId2);
      buf.writeInt(rowId3);

      byte[] bytes = func.getClass().getName().getBytes();
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
      func.serialize(buf);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId1 = buf.readInt();
      rowId2 = buf.readInt();
      rowId3 = buf.readInt();

      int size = buf.readInt();
      byte[] bytes = new byte[size];
      buf.readBytes(bytes);
      try {
        func = (Serialize) Class.forName(new String(bytes)).newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
      func.deserialize(buf);
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 12
          + (4 + func.getClass().getName().getBytes().length + func.bufferLen());
    }

    public int getRowId1() {
      return rowId1;
    }

    public int getRowId2() {
      return rowId2;
    }

    public int getRowId3() {
      return rowId3;
    }

    public Serialize getFunc() {
      return func;
    }

    @Override
    public String toString() {
      return "V3FPartitionUpdaterParam [rowId1=" + rowId1 + ", rowId2=" + rowId2
          + ", rowId3=" + rowId3 + ", func=" + func.getClass().getName()
          + ", toString()=" + super.toString() + "]";
    }
  }

  private final int rowId1;
  private final int rowId2;
  private final int rowId3;
  private final Serialize func;

  public V3FUpdaterParam(int matrixId, int rowId1, int rowId2, int rowId3, Serialize func) {
    super(matrixId, false);
    this.rowId1 = rowId1;
    this.rowId2 = rowId2;
    this.rowId3 = rowId3;
    this.func = func;
  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId);

    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      if (rowId1 < part.getStartRow() || rowId1 >= part.getEndRow() ||
          rowId2 < part.getStartRow() || rowId2 >= part.getEndRow() ||
          rowId3 < part.getStartRow() || rowId3 >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new V3FPartitionUpdaterParam(matrixId, part, rowId1, rowId2, rowId3, func));
    }

    return partParams;
  }

}
