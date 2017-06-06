package com.tencent.angel.ml.matrix.psf.get.single;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Partition parameter of get row function.
 */
public class PartitionGetRowParam extends PartitionGetParam {
  /** row index */
  private int rowIndex;

  /**
   * Create a new PartitionGetRowParam.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param partKey matrix partition key
   */
  public PartitionGetRowParam(int matrixId, int rowIndex, PartitionKey partKey) {
    super(matrixId, partKey);
    this.rowIndex = rowIndex;
  }

  /**
   * Create a new PartitionGetRowParam.
   */
  public PartitionGetRowParam() {
    this(-1, -1, null);
  }

  /**
   * Get row index.
   *
   * @return int row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(rowIndex);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    rowIndex = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return 4 + super.bufferLen();
  }
}
