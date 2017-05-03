package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.aggr.AggrParam;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class UnaryAggrParam extends AggrParam {

  public static class UnaryPartitionAggrParam extends PartitionAggrParam {
    private int rowId;

    public UnaryPartitionAggrParam(int matrixId, PartitionKey partKey, int rowId) {
      super(matrixId, partKey);
      this.rowId = rowId;
    }

    public UnaryPartitionAggrParam() {
      this(0, null, 0);
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 4;
    }

    public int getRowId() {
      return rowId;
    }

  }

  private final int rowId;

  public UnaryAggrParam(int matrixId, int rowId) {
    super(matrixId);
    this.rowId = rowId;
  }

  @Override
  public List<PartitionAggrParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = parts.size();

    List<PartitionAggrParam> partParams = new ArrayList<PartitionAggrParam>(size);

    for (PartitionKey part : parts) {
      if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new UnaryPartitionAggrParam(matrixId, part, rowId));
    }

    return partParams;
  }

}
