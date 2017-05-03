package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class V2UpdaterParam extends UpdaterParam {

  public static class V2PartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId1;
    private int rowId2;

    public V2PartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int rowId1, int rowId2) {

      super(matrixId, partKey, false);
      this.rowId1 = rowId1;
      this.rowId2 = rowId2;
    }

    public V2PartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId1);
      buf.writeInt(rowId2);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId1 = buf.readInt();
      rowId2 = buf.readInt();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 8;
    }

    public int getRowId1() {
      return rowId1;
    }

    public int getRowId2() {
      return rowId2;
    }

    @Override
    public String toString() {
      return "V2PartitionUpdaterParam [rowId1=" + rowId1 + ", rowId2=" + rowId2 + ", toString()="
          + super.toString() + "]";
    }
  }

  private final int rowId1;
  private final int rowId2;

  public V2UpdaterParam(int matrixId, int rowId1, int rowId2) {
    super(matrixId, false);
    this.rowId1 = rowId1;
    this.rowId2 = rowId2;
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
      partParams.add(new V2PartitionUpdaterParam(matrixId, part, rowId1, rowId2));
    }

    return partParams;
  }

}
