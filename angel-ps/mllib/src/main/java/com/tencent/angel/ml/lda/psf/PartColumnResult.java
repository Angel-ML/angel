package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PartColumnResult extends PartitionGetResult {

  private ByteBuf buf;
  public Map<Long, Int2IntOpenHashMap> cks;

  public PartColumnResult(Map<Long, Int2IntOpenHashMap> cks) {
    this.cks = cks;
  }

  public PartColumnResult() {}

  @Override
  public void serialize(ByteBuf buf) {
    Iterator<Long> keyIterator = cks.keySet().iterator();

    buf.writeInt(cks.size());

    while (keyIterator.hasNext()) {
      long column = keyIterator.next();
      buf.writeLong(column);
      Int2IntOpenHashMap ck = cks.get(column);
      buf.writeInt(ck.size());
      ObjectIterator<Int2IntMap.Entry> iter = ck.int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    }
  }

  @Override
  public int bufferLen() { return 16;}

  @Override
  public void deserialize(ByteBuf buf) {
    int numColumns = buf.readInt();

    cks = new HashMap();
    for (int i = 0; i < numColumns; i ++) {
      long column = buf.readLong();
      int size    = buf.readInt();
      Int2IntOpenHashMap ck = new Int2IntOpenHashMap(size);
      for (int j = 0; j < size; j ++) {
        ck.put(buf.readInt(), buf.readInt());
      }

      cks.put(column, ck);
    }
  }

  public void merge(PartColumnResult other) {
    Iterator<Long> keyIterator = other.cks.keySet().iterator();
    while (keyIterator.hasNext()) {
      long column = keyIterator.next();
      if (cks.containsKey(column)) {
        cks.get(column).putAll(other.cks.get(column));
      } else {
        cks.put(column, other.cks.get(column));
      }
    }
  }
}
