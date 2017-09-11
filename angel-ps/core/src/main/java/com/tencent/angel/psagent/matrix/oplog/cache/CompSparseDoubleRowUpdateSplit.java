package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;


/**
 * Row split of component sparse double row update.
 */
public class CompSparseDoubleRowUpdateSplit extends RowUpdateSplit {
  private final SparseDoubleVector split;

  /**
   * Create a new CompSparseDoubleRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param rowType  row type
   */
  public CompSparseDoubleRowUpdateSplit(SparseDoubleVector split, int rowIndex,
    MLProtos.RowType rowType) {
    super(rowIndex, rowType, -1, -1);
    this.split = split;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(split.size());

    ObjectIterator<Int2DoubleMap.Entry> iter =
      split.getIndexToValueMap().int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      buf.writeInt(entry.getIntKey());
      buf.writeDouble(entry.getDoubleValue());
    }
  }

  @Override public long size() {
    return split.size();
  }

  @Override public int bufferLen() {
    return super.bufferLen() + split.size() * 12;
  }
}
