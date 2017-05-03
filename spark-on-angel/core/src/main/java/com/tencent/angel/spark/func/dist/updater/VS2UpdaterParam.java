package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class VS2UpdaterParam extends UpdaterParam {

  public static class VS2PartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId;
    private double scalar1;
    private double scalar2;

    public VS2PartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int rowId, double scalar1, double scalar2) {
      super(matrixId, partKey, false);
      this.rowId = rowId;
      this.scalar1 = scalar1;
      this.scalar2 = scalar2;
    }

    public VS2PartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeDouble(scalar1);
      buf.writeDouble(scalar2);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      scalar1 = buf.readDouble();
      scalar2 = buf.readDouble();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 20;
    }

    public int getRowId() {
      return rowId;
    }

    public double getScalar1() {
      return scalar1;
    }

    public double getScalar2() {
      return scalar2;
    }

    @Override
    public String toString() {
      return "VS2PartitionUpdaterParam [rowId=" + rowId + ", scalar1=" + scalar1
          + ", scalar2=" + scalar2 + ", toString()="
          + super.toString() + "]";
    }
  }

  private final int rowId;
  private final double scalar1;
  private final double scalar2;

  public VS2UpdaterParam(int matrixId, int rowId, double scalar1, double scalar2) {
    super(matrixId, false);
    this.rowId = rowId;
    this.scalar1 = scalar1;
    this.scalar2 = scalar2;
  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new VS2PartitionUpdaterParam(matrixId, part, rowId, scalar1, scalar2));
    }

    return partParams;
  }

}
