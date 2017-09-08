package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Sparse double vector partition with long key.
 */
public class ServerSparseDoubleLongKeyRow extends ServerRow{
  private final static Log LOG = LogFactory.getLog(ServerSparseDoubleLongKeyRow.class);

  /** Index->Value map */
  private volatile Long2DoubleOpenHashMap index2ValueMap;

  /**
   * Create a ServerSparseDoubleLongKeyRow
   * @param rowId row index
   * @param startCol vector partition start position
   * @param endCol vector partition end position
   */
  public ServerSparseDoubleLongKeyRow(int rowId, long startCol, long endCol) {
    super(rowId, startCol, endCol);
    index2ValueMap = new Long2DoubleOpenHashMap();
  }

  /**
   * Create a ServerSparseDoubleLongKeyRow
   */
  public ServerSparseDoubleLongKeyRow() {
    this(0, 0, 0);
  }

  @Override public MLProtos.RowType getRowType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  public Long2DoubleOpenHashMap getIndex2ValueMap() {
    return index2ValueMap;
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeInt(index2ValueMap.size());
      for (Long2DoubleMap.Entry entry : index2ValueMap.long2DoubleEntrySet()) {
        output.writeLong(entry.getLongKey());
        output.writeDouble(entry.getDoubleValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void readFrom(DataInputStream input) throws IOException {
    try {
      lock.writeLock().lock();
      super.readFrom(input);
      int nnz = input.readInt();
      if(index2ValueMap.size() < nnz) {
        index2ValueMap = new Long2DoubleOpenHashMap(nnz);
      }
      for (int i = 0; i < nnz; i++) {
        index2ValueMap.addTo(input.readLong(), input.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return index2ValueMap.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(MLProtos.RowType rowType, ByteBuf buf, int size) {
    try {
      lock.writeLock().lock();
      switch (rowType) {
        case T_DOUBLE_SPARSE_LONGKEY:
          updateDoubleSparse(buf, size);
          break;

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }

      updateRowVersion();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void resizeHashMap(int size) {
    if(index2ValueMap.size() < size) {
      Long2DoubleOpenHashMap oldMap = index2ValueMap;
      index2ValueMap = new Long2DoubleOpenHashMap(size);
      index2ValueMap.putAll(oldMap);
    }
  }

  private void updateDoubleSparse(ByteBuf buf, int size) {
    resizeHashMap(size);
    for (int i = 0; i < size; i++) {
      index2ValueMap.addTo(buf.readLong(), buf.readDouble());
    }
  }

  public Long2DoubleOpenHashMap getData() {
    return index2ValueMap;
  }

  @Override
  public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeInt(index2ValueMap.size());

      ObjectIterator<Long2DoubleMap.Entry> iter = index2ValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    try {
      lock.writeLock().lock();
      super.deserialize(buf);
      int elemNum = buf.readInt();
      index2ValueMap = new Long2DoubleOpenHashMap(elemNum);
      for (int i = 0; i < elemNum; i++) {
        index2ValueMap.put(buf.readLong(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + index2ValueMap.size() * 16;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Merge a dense double vector to this vector partition
   * @param size the elements number of the dense double vector
   * @param buf  serialized dense double vector
   */
  public void mergeDoubleDense(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      resizeHashMap(size);
      for (int i = 0; i < size; i++) {
        index2ValueMap.addTo(i, buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge a sparse double vector to this vector partition
   * @param size the elements number of the sparse double vector
   * @param buf serialized sparse double vector
   */
  public void mergeDoubleSparse(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      resizeHashMap(size);
      for (int i = 0; i < size; i++) {
        index2ValueMap.addTo(buf.readLong(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Put all elements in this vector partition to a given map
   * @param other a long->double map
   */
  public void mergeTo(Long2DoubleOpenHashMap other) {
    try {
      lock.readLock().lock();
      other.putAll(index2ValueMap);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Put the elements in this vector partition to the given index and value arrays
   * @param indexes index array
   * @param values value array
   * @param startPos the start position for the elements of this vector partition
   * @param len the reserved length for this vector partition
   */
  public void mergeTo(long[] indexes, double[] values, int startPos, int len) {
    try {
      lock.readLock().lock();
      int writeLen = len < index2ValueMap.size() ? len : index2ValueMap.size();
      if (writeLen == 0) {
        return;
      }

      int index = 0;

      ObjectIterator<Long2DoubleMap.Entry> iter = index2ValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexes[startPos + index] = entry.getLongKey();
        values[startPos + index] = entry.getDoubleValue();
        index++;
        if (index == writeLen) {
          return;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }
}
