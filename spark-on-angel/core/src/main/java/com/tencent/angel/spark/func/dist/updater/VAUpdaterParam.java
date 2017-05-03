package com.tencent.angel.spark.func.dist.updater;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterParam;

public class VAUpdaterParam extends UpdaterParam {

  public static class VAPartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId;
    private int start;
    private int end;
    private double[] array;
    private double[] arraySlice;

    public VAPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int rowId, int start, int end, double[] array) {
      super(matrixId, partKey, false);
      this.rowId = rowId;
      this.start = start;
      this.end = end;
      this.array = array;
    }

    public VAPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(end - start);
      for (int i = start; i < end; i++) {
        buf.writeDouble(array[i]);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      int length = buf.readInt();
      arraySlice = new double[length];
      for (int i = 0; i < length; i++) {
        arraySlice[i] = buf.readDouble();
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 8 + (end - start) * 8;
    }

    public int getRowId() {
      return rowId;
    }

    public double[] getArraySlice() {
      return arraySlice;
    }

    @Override
    public String toString() {
      return "VAPartitionUpdaterParam [rowId=" + rowId + ", toString()="
          + super.toString() + "]";
    }
  }

  private final int rowId;
  private final double[] array;


  public VAUpdaterParam(int matrixId, int rowId, double[] array) {
    super(matrixId, false);
    this.rowId = rowId;
    this.array = array;

  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId);

    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new VAPartitionUpdaterParam(matrixId, part, rowId,
          part.getStartCol(), part.getEndCol(), array));
    }

    return partParams;
  }

}
