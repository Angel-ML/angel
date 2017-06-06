package com.tencent.angel.ml.matrix.psf.get.multi;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Partition parameter of get rows function.
 */
public class PartitionGetRowsParam extends PartitionGetParam {
  /** row indexes */
  private List<Integer> rowIndexes;

  /**
   * Create a new PartitionGetRowsParam.
   *
   * @param matrixId matrix id
   * @param partKey matrix partition key
   * @param rowIndexes row indexes
   */
  public PartitionGetRowsParam(int matrixId, PartitionKey partKey, List<Integer> rowIndexes) {
    super(matrixId, partKey);
    this.setRowIndexes(rowIndexes);
  }

  /**
   * Create a new PartitionGetRowsParam.
   */
  public PartitionGetRowsParam() {
    this(-1, null, null);
  }

  /**
   * Get row indexes.
   *
   * @return List<Integer> row indexes
   */
  public List<Integer> getRowIndexes() {
    return rowIndexes;
  }

  /**
   * Set row indexes.
   *
   * @param rowIndexes row indexes
   */
  public void setRowIndexes(List<Integer> rowIndexes) {
    this.rowIndexes = rowIndexes;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowIndexes != null) {
      int size = rowIndexes.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowIndexes.get(i));
      }
    } else {
      buf.writeInt(0);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = buf.readInt();
    rowIndexes = new ArrayList<Integer>(size);
    for (int i = 0; i < size; i++) {
      rowIndexes.add(buf.readInt());
    }
  }

  @Override
  public int bufferLen() {
    return 4 + ((rowIndexes != null) ? rowIndexes.size() * 4 : 0);
  }
}
