package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class V2FUpdaterParam extends UpdaterParam {

  public static class V2FPartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId1;
    private int rowId2;
    private Serialize func;

    public V2FPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int rowId1, int rowId2, Serialize func) {
      super(matrixId, partKey, false);
      this.rowId1 = rowId1;
      this.rowId2 = rowId2;
      this.func = func;
    }

    public V2FPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId1);
      buf.writeInt(rowId2);

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
      return super.bufferLen() + 8
          + (4 + func.getClass().getName().getBytes().length + func.bufferLen());
    }

    public int getRowId1() {
      return rowId1;
    }

    public int getRowId2() {
      return rowId2;
    }

    public Serialize getFunc() {
      return func;
    }

    @Override
    public String toString() {
      return "V2FPartitionUpdaterParam [rowId1=" + rowId1 + ", rowId2=" + rowId2
          + ", func=" + func.getClass().getName() + ", toString()="
          + super.toString() + "]";
    }
  }

  private final int rowId1;
  private final int rowId2;
  private final Serialize func;

  public V2FUpdaterParam(int matrixId, int rowId1, int rowId2, Serialize func) {
    super(matrixId, false);
    this.rowId1 = rowId1;
    this.rowId2 = rowId2;
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
          rowId2 < part.getStartRow() || rowId2 >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new V2FPartitionUpdaterParam(matrixId, part, rowId1, rowId2, func));
    }

    return partParams;
  }

}
