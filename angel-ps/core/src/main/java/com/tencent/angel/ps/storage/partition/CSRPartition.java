package com.tencent.angel.ps.storage.partition;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.partition.op.ICSRStorageOp;
import com.tencent.angel.ps.storage.partition.storage.CSRStorage;
import io.netty.buffer.ByteBuf;

public class CSRPartition extends ServerPartition implements ICSRStorageOp {
  /**
   * Create new CSRPartition
   *
   * @param partKey partition key
   * @param storage row-based matrix partition storage
   * @param rowType row type
   */
  public CSRPartition(PartitionKey partKey, double estSparsity, CSRStorage storage,
      RowType rowType) {
    super(partKey, rowType, estSparsity, storage);
  }

  public CSRPartition() {
    this(null, 0.0, null, RowType.T_DOUBLE_DENSE);
  }

  @Override
  public CSRStorage getStorage() {
    return (CSRStorage) super.getStorage();
  }

  @Override
  public void update(ByteBuf buf, UpdateOp op) {
    throw new UnsupportedOperationException("pipeline update not support for CSRPartition now");
  }

  @Override
  public void init() {
    getStorage().init(partKey);
  }

  @Override
  public void reset() {
    getStorage().reset();
  }
}
