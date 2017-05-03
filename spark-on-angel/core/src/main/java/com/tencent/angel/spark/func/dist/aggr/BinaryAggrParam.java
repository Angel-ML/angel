package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.aggr.AggrParam;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class BinaryAggrParam extends AggrParam {

  public static class BinaryPartitionAggrParam extends PartitionAggrParam {
    private int rowId1;
    private int rowId2;

    public BinaryPartitionAggrParam(int matrixId, PartitionKey partKey, int rowId1, int rowId2) {
      super(matrixId, partKey);
      this.rowId1 = rowId1;
      this.rowId2 = rowId2;
    }

    public BinaryPartitionAggrParam() {
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
  }

  private final int rowId1;
  private final int rowId2;

  public BinaryAggrParam(int matrixId, int rowId1, int rowId2) {
    super(matrixId);
    this.rowId1 = rowId1;
    this.rowId2 = rowId2;
  }

  @Override
  public List<PartitionAggrParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = parts.size();

    List<PartitionAggrParam> partParams = new ArrayList<PartitionAggrParam>(size);

    for (PartitionKey part : parts) {
      if (rowId1 < part.getStartRow() || rowId1 >= part.getEndRow() ||
          rowId2 < part.getStartRow() || rowId2 >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new BinaryPartitionAggrParam(matrixId, part, rowId1, rowId2));
    }

    return partParams;
  }

}
