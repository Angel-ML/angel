package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math.vector.SparseFloatVector;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Row split of component sparse float row update.
 */
public class CompSparseFloatRowUpdateSplit extends RowUpdateSplit {
  private final SparseFloatVector split;

  /**
   * Create a new CompSparseFloatRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param rowType  row type
   */
  public CompSparseFloatRowUpdateSplit(SparseFloatVector split, int rowIndex,
    MLProtos.RowType rowType) {
    super(rowIndex, rowType, -1, -1);
    this.split = split;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(split.size());
    LOG.debug("double size = " + split.size());

    ObjectIterator<Int2FloatMap.Entry> iter =
      split.getIndexToValueMap().int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      buf.writeInt(entry.getIntKey());
      buf.writeFloat(entry.getFloatValue());
    }
  }

  @Override public long size() {
    return split.size();
  }

  @Override public int bufferLen() {
    return super.bufferLen() + split.size() * 8;
  }
}
