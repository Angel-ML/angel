package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;

public class LongKeySparseDoubleRowUpdateSplit extends RowUpdateSplit{
  /** indexes */
  private final long[] offsets;

  /** values of row */
  private final double[] values;

  /**
   * Create a new RowUpdateSplit.
   *
   * @param rowIndex row index
   * @param rowType  row type
   * @param start    split start position
   * @param end      split end position
   */
  public LongKeySparseDoubleRowUpdateSplit(int rowIndex, MLProtos.RowType rowType, int start,
    int end, long[] offsets, double[] values) {
    super(rowIndex, rowType, start, end);
    this.offsets = offsets;
    this.values = values;
  }

  /**
   * Get indexes of row values
   *
   * @return int[] indexes of row values
   */
  public long[] getOffsets() {
    return offsets;
  }

  /**
   * Get row values
   *
   * @return double[] row values
   */
  public double[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(end - start);
    LOG.debug("double size = " + (end - start));
    for (int i = start; i < end; i++) {
      buf.writeLong(offsets[i]);
      buf.writeDouble(values[i]);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + (end - start) * 16;
  }
}
