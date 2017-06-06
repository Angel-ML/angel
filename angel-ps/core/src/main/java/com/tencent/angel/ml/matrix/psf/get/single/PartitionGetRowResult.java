package com.tencent.angel.ml.matrix.psf.get.single;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.impl.matrix.*;
import io.netty.buffer.ByteBuf;

/**
 * The result of partition get row function.
 */
public class PartitionGetRowResult extends PartitionGetResult {
  /** row split */
  private ServerRow rowSplit;

  /**
   * Create a new PartitionGetRowResult.
   *
   * @param rowSplit row split
   */
  public PartitionGetRowResult(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }

  /**
   * Create a new PartitionGetRowResult.
   *
   */
  public PartitionGetRowResult() {
    this(null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    if (rowSplit != null) {
      buf.writeInt(rowSplit.getRowType().getNumber());
      rowSplit.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    if (buf.readableBytes() == 0) {
      rowSplit = null;
      return;
    }

    MLProtos.RowType type = MLProtos.RowType.valueOf(buf.readInt());
    if (rowSplit == null) {
      switch (type) {
        case T_DOUBLE_DENSE: {
          rowSplit = new ServerDenseDoubleRow();
          break;
        }
        case T_DOUBLE_SPARSE: {
          rowSplit = new ServerSparseDoubleRow();
          break;
        }

        case T_INT_DENSE: {
          rowSplit = new ServerDenseIntRow();
          break;
        }

        case T_INT_SPARSE: {
          rowSplit = new ServerSparseIntRow();
          break;
        }

        case T_FLOAT_DENSE: {
          rowSplit = new ServerDenseFloatRow();
          break;
        }
        default:
          break;
      }
    }

    rowSplit.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    if (rowSplit != null)
      return rowSplit.bufferLen();
    else
      return 0;
  }

  /**
   * Get row split.
   *
   * @return ServerRow row split
   */
  public ServerRow getRowSplit() {
    return rowSplit;
  }
}
