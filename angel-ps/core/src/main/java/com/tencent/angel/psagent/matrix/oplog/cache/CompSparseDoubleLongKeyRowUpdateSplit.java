package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math.vector.SparseDoubleLongKeyVector;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Row split of component sparse double long key row update.
 */
public class CompSparseDoubleLongKeyRowUpdateSplit extends RowUpdateSplit {

  private SparseDoubleLongKeyVector split;

  /**
   * Create a new CompSparseDoubleLongKeyRowUpdateSplit
   *
   * @param rowIndex row index
   * @param rowType  row type
   */
  public CompSparseDoubleLongKeyRowUpdateSplit(SparseDoubleLongKeyVector split, int rowIndex,
    MLProtos.RowType rowType) {
    super(rowIndex, rowType, -1, -1);
    this.split = split;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(split.size());
    LOG.debug("double size = " + split.size());

    ObjectIterator<Long2DoubleMap.Entry> iter =
      split.getIndexToValueMap().long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      buf.writeLong(entry.getLongKey());
      buf.writeDouble(entry.getDoubleValue());
    }
  }

  @Override public int bufferLen() {
    return super.bufferLen() + split.size() * 16;
  }
}
