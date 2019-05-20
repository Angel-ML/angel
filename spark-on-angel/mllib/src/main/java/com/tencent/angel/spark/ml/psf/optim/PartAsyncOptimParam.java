package com.tencent.angel.spark.ml.psf.optim;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.update.PartUpdateRowsParam;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class PartAsyncOptimParam extends PartUpdateRowsParam {
  private double[] doubles;
  private int[] ints;

  public PartAsyncOptimParam(int matrixId, PartitionKey part, List<RowUpdateSplit> updates, double[] doubles, int[] ints) {
    super(matrixId, part, updates);
    this.doubles = doubles;
    this.ints = ints;
  }

  public PartAsyncOptimParam() {
    this(-1, null, null, null, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);

    buf.writeInt(doubles.length);
    for (int i = 0; i < doubles.length; i++)
      buf.writeDouble(doubles[i]);

    buf.writeInt(ints.length);
    for (int i = 0; i < ints.length; i++)
      buf.writeInt(ints[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    int size;
    size = buf.readInt();
    doubles = new double[size];
    for (int i = 0; i < size; i++)
      doubles[i] = buf.readDouble();

    size = buf.readInt();
    ints = new int[size];
    for (int i = 0; i < size; i++)
      ints[i] = buf.readInt();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    len += doubles.length * 8;
    len += ints.length * 4;
    return len;
  }

  public double[] getDoubles() {
    return doubles;
  }

  public int[] getInts() {
    return ints;
  }

}
