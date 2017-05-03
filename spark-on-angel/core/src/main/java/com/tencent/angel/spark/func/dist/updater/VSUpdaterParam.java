package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class VSUpdaterParam extends UpdaterParam {

  public static class VSPartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId;
    private double scalar;

    public VSPartitionUpdaterParam(int matrixId, PartitionKey partKey, int rowId, double scalar) {
      super(matrixId, partKey, false);
      this.rowId = rowId;
      this.scalar = scalar;
    }

    public VSPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeDouble(scalar);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      scalar = buf.readDouble();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 12;
    }

    public int getRowId() {
      return rowId;
    }

    public double getScalar() {
      return scalar;
    }

    @Override
    public String toString() {
      return "VSPartitionUpdaterParam [rowId=" + rowId + ", scalar=" + scalar + ", toString()="
          + super.toString() + "]";
    }
  }

  private final int rowId;
  private final double scalar;

  public VSUpdaterParam(int matrixId, int rowId, double scalar) {
    super(matrixId, false);
    this.rowId = rowId;
    this.scalar = scalar;
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
      partParams.add(new VSPartitionUpdaterParam(matrixId, part, rowId, scalar));
    }

    return partParams;
  }

}
