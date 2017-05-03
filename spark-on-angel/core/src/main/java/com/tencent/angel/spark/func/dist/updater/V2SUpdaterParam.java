package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class V2SUpdaterParam extends UpdaterParam {

  public static class V2SPartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId1;
    private int rowId2;
    private double scalar;

    public V2SPartitionUpdaterParam(int matrixId, PartitionKey partKey, int rowId1,
                                    int rowId2, double scalar) {
      super(matrixId, partKey, false);
      this.rowId1 = rowId1;
      this.rowId2 = rowId2;
      this.scalar = scalar;
    }

    public V2SPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId1);
      buf.writeInt(rowId2);
      buf.writeDouble(scalar);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId1 = buf.readInt();
      rowId2 = buf.readInt();
      scalar = buf.readDouble();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 16;
    }

    public int getRowId1() {
      return rowId1;
    }

    public int getRowId2() {
      return rowId2;
    }

    public double getScalar() {
      return scalar;
    }

    @Override
    public String toString() {
      return "V2SPartitionUpdaterParam [rowId1=" + rowId1 + ", rowId2=" + rowId2
          + ", scalar=" + scalar + ", toString()=" + super.toString() + "]";
    }
  }

  private final int rowId1;
  private final int rowId2;
  private final double scalar;

  public V2SUpdaterParam(int matrixId, int rowId1, int rowId2, double scalar) {
    super(matrixId, false);
    this.rowId1 = rowId1;
    this.rowId2 = rowId2;
    this.scalar = scalar;
  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList = PSAgentContext.get().getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId);
    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      if (rowId1 < part.getStartRow() || rowId1 >= part.getEndRow() ||
          rowId2 < part.getStartRow() || rowId2 >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new V2SPartitionUpdaterParam(matrixId, part, rowId1, rowId2, scalar));
    }

    return partParams;
  }

}
