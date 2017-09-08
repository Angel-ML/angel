package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Row split of component sparse int row update.
 */
public class CompSparseIntRowUpdateSplit extends RowUpdateSplit{
  private SparseIntVector split;
  /**
   * Create a new CompSparseIntRowUpdateSplit.
   *
   * @param rowIndex row index
   * @param rowType  row type
   */
  public CompSparseIntRowUpdateSplit(SparseIntVector split, int rowIndex, MLProtos.RowType rowType) {
    super(rowIndex, rowType, -1, -1);
    this.split = split;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(split.size());
    LOG.debug("double size = " + split.size());

    ObjectIterator<Int2IntMap.Entry>
      iter = split.getIndexToValueMap().int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      buf.writeInt(entry.getIntKey());
      buf.writeInt(entry.getIntValue());
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + split.size() * 8;
  }
}
