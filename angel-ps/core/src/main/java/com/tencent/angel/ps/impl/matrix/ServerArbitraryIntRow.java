/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;


/**
 * The class represent arbitrary int row on parameter server. The row can convert between dense and
 * sparse, depending on the data density and the threshold is 0.5.
 */
public class ServerArbitraryIntRow extends ServerRow {

  private final static Log LOG = LogFactory.getLog(ServerArbitraryIntRow.class);
  private final static double threshold = 0.5;
  private byte[] buf;
  private IntBuffer denseRep;
  private Int2IntOpenHashMap sparseRep;
  private int nnz;

  /**
   * Create a new Server arbitrary int row.
   *
   * @param rowId the row id
   * @param startCol the start col
   * @param endCol the end col
   */
  public ServerArbitraryIntRow(int rowId, int startCol, int endCol) {
    super(rowId, startCol, endCol);
    this.nnz = 0;
    this.sparseRep = null;
    this.denseRep = null;
  }

  /**
   * Create a new Server arbitrary int row.
   */
  public ServerArbitraryIntRow() {}

  private void initDenseRep() {
    if (denseRep == null) {
      int length = (int)(endCol - startCol);
      buf = new byte[length * 4];
      denseRep = wrapIntBuffer(buf);
    }
  }

  private void initDenseRep(byte[] buf) {
    this.buf = buf;
    denseRep = wrapIntBuffer(buf);
    nnz = denseRep.capacity();
  }

  private void initDenseRep(ByteBuf buf, int len) {
    this.buf = new byte[len * 4];
    denseRep = wrapIntBuffer(this.buf);
    nnz = 0;
    for (int i = 0; i < len; i++) {
      int v = buf.readInt();
      if (v != 0) {
        denseRep.put(i, v);
        nnz++;
      }
    }

    if (nnz < threshold * len)
      denseToSparse();
  }

  private void initSparseRep() {
    if (sparseRep == null)
      sparseRep = new Int2IntOpenHashMap();
    nnz = 0;
  }

  private void initSparseRep(int size) {
    if (sparseRep == null)
      sparseRep = new Int2IntOpenHashMap(size);
    nnz = 0;
  }

  private void initSparseRep(IntBuffer keys, IntBuffer value) {
    initSparseRep(nnz);
    for (int i = 0; i < nnz; i++) {
      sparseRep.put(keys.get(i), value.get(i));
    }
    nnz = sparseRep.size();
  }

  private void denseToSparse() {
    initSparseRep();
    int length = (int)(endCol - startCol);
    for (int i = 0; i < length; i++) {
      int v = denseRep.get(i);
      if (v != 0)
        sparseRep.put(i, v);
    }
    nnz = sparseRep.size();
    denseRep = null;
    buf = null;
  }

  private void densePlusDense(IntBuffer buf) {
    int length = buf.capacity();
    nnz = 0;
    int value;
    for (int i = 0; i < length; i++) {
      value = denseRep.get(i) + buf.get(i);
      if (value != 0) {
        denseRep.put(i, denseRep.get(i) + buf.get(i));
        nnz++;
      }
    }

    if (nnz < threshold)
      denseToSparse();
  }

  private void densePlusSparse(IntBuffer keys, IntBuffer values) {
    int length = keys.capacity();
    int ov, value, key;
    for (int i = 0; i < length; i++) {
      key = keys.get(i);
      ov = denseRep.get(key);
      value = ov + values.get(i);
      if (ov != 0 && value == 0)
        nnz--;
      denseRep.put(key, value);
    }

    int size = (int)(endCol - startCol);
    if (nnz < threshold * size)
      denseToSparse();
  }

  private void sparseToDense() {
    initDenseRep();
    ObjectIterator<Entry> iter = sparseRep.int2IntEntrySet().fastIterator();
    nnz = 0;
    while (iter.hasNext()) {
      Entry itemEntry = iter.next();
      denseRep.put(itemEntry.getIntKey(), itemEntry.getIntValue());
      if(itemEntry.getIntValue() != 0) {
        nnz++;
      }
    }
    sparseRep = null;

    // int[] keys = sparseRep.getKeys();
    // int[] values = sparseRep.getValues();
    // boolean[] used = sparseRep.getUsed();
    // nnz = 0;
    // for (int i = 0; i < keys.length; i++)
    // if (used[i]) {
    // denseRep.put(keys[i], values[i]);
    // nnz++;
    // }
    // sparseRep = null;
  }

  private void sparsePlusDense(byte[] buf) {
    initDenseRep(buf);

    ObjectIterator<Entry> iter = sparseRep.int2IntEntrySet().fastIterator();
    while (iter.hasNext()) {
      Entry itemEntry = iter.next();
      denseRep.put(itemEntry.getIntKey(), itemEntry.getIntValue());
    }
    sparseRep = null;

    // int[] keys = sparseRep.getKeys();
    // int[] values = sparseRep.getValues();
    // boolean[] used = sparseRep.getUsed();
    // for (int i = 0; i < keys.length; i++)
    // if (used[i]) {
    // denseRep.put(keys[i], values[i]);
    // }
    // sparseRep = null;
  }

  private void sparsePlusSparse(IntBuffer keys, IntBuffer values) {
    int length = keys.capacity();
    int key, value;
    for (int i = 0; i < length; i++) {
      key = keys.get(i);
      value = values.get(i);
      sparseRep.addTo(key, value);
    }

    nnz = sparseRep.size();
    int size = (int)(endCol - startCol);
    if (nnz > threshold * size)
      sparseToDense();
  }

  @Override
  public RowType getRowType() {
    if (sparseRep != null)
      return RowType.T_INT_SPARSE;
    else
      return RowType.T_INT_DENSE;
  }

  @SuppressWarnings("unused")
  private void update(IntBuffer keys, IntBuffer values) {
    if (sparseRep == null && denseRep == null) {
      initSparseRep(keys, values);
    } else if (sparseRep != null) {
      sparsePlusSparse(keys, values);
    } else {
      densePlusSparse(keys, values);
    }
  }

  @SuppressWarnings("unused")
  private void update(byte[] values) {
    if (sparseRep == null && denseRep == null) {
      initDenseRep(values);
    } else if (denseRep != null) {
      densePlusDense(wrapIntBuffer(values));
    } else {
      sparsePlusDense(values);
    }
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    if (sparseRep != null)
      writeSparseTo(output);
    else if (denseRep != null)
      writeDenseTo(output);
  }

  public void writeDenseTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeUTF(RowType.T_INT_DENSE.toString());
      int data[] = denseRep.array();
      // output.writeInt(data.length);
      for (int i = 0; i < data.length; i++) {
        output.writeInt(data[i]);
      }
    } finally {
      lock.readLock().unlock();
    }

  }

  public void writeSparseTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeUTF(RowType.T_INT_SPARSE.toString());
      output.writeInt(nnz);
      IntSet keys = sparseRep.keySet();
      output.writeInt(keys.size());
      for (int key : keys) {
        output.writeInt(key);
        output.writeInt(sparseRep.get(key));
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void readFrom(DataInputStream input) throws IOException {
    if (sparseRep != null)
      readSparseFrom(input);
    else if (denseRep != null)
      readDenseFrom(input);
  }

  public void readDenseFrom(DataInputStream input) {

  }

  public void readSparseFrom(DataInputStream input) {

  }

  // @Override
  // public void serialize(ByteBuf buf) {
  // if (sparseRep != null)
  // return serializeSparse();
  // else if (denseRep != null)
  // return serializeDense();
  // return serializeEmpty();
  // }

  private byte[] serializeSparse() {
    int length = 8 * nnz;
    byte[] bytes = new byte[length];

    IntBuffer keysBuf = wrapIntBuffer(bytes, 0, nnz * 4);
    IntBuffer valuesBuf = wrapIntBuffer(bytes, nnz * 4, nnz * 4);

    ObjectIterator<Entry> iter = sparseRep.int2IntEntrySet().fastIterator();
    int idx = 0;
    while (iter.hasNext()) {
      Entry itemEntry = iter.next();
      keysBuf.put(idx, itemEntry.getIntKey());
      valuesBuf.put(idx, itemEntry.getIntValue());
      idx++;
    }

    // int[] keys = sparseRep.getKeys();
    // int[] values = sparseRep.getValues();
    // boolean[] used = sparseRep.getUsed();

    // int idx = 0;
    // for (int i = 0; i < keys.length; i++)
    // if (used[i]) {
    // keysBuf.put(idx, keys[i]);
    // valuesBuf.put(idx, values[i]);
    // idx++;
    // }

    return bytes;
  }

  private byte[] serializeDense() {
    return buf;
  }

  private byte[] serializeEmpty() {
    return new byte[0];
  }

  private IntBuffer wrapIntBuffer(byte[] data) {
    return wrapIntBuffer(data, 0, data.length);
  }

  private IntBuffer wrapIntBuffer(byte[] data, int offset, int length) {
    return ByteBuffer.wrap(data, offset, length).asIntBuffer();
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      if (denseRep != null) {
        return denseRep.capacity();
      } else if (sparseRep != null) {
        return sparseRep.size();
      } else {
        return 0;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      if (denseRep != null) {
        return super.bufferLen() + buf.length;
      } else if (sparseRep != null) {
        return super.bufferLen() + nnz * 12;
      } else {
        return super.bufferLen();
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();
    try {
      switch (rowType) {
        case T_INT_DENSE:
          updateIntDense(buf);
          break;
        case T_INT_SPARSE:
          updateIntSparse(buf);
          break;
        default:
          LOG.error("Invalid rowType to update ServerDenseIntRow!");
      }
      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  public void updateIntSparse(ByteBuf buf) {
    int size = buf.readInt();
    if (sparseRep == null && denseRep == null) {
      initSparseRep(buf, size);
    } else if (sparseRep != null) {
      sparsePlusSparse(buf, size);
    } else {
      densePlusSparse(buf, size);
    }
  }

  private void densePlusSparse(ByteBuf buf, int size) {
    ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 4);

    int ov, value, key, delta;
    for (int i = 0; i < size; i++) {
      key = buf.readInt();
      ov = denseRep.get(key);
      delta = valueBuf.readInt();
      value = ov + delta;

      if (ov != 0 && value == 0)
        nnz--;
      denseRep.put(key, value);
    }

    buf.readerIndex(buf.readerIndex() + size * 4);

    LOG.info("#######nnz=" + nnz);
    if (nnz < threshold * (endCol - startCol))
      denseToSparse();
  }

  private void sparsePlusSparse(ByteBuf buf, int size) {
    ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 4);

    for (int i = 0; i < size; i++) {
      sparseRep.addTo(buf.readInt(), valueBuf.readInt());
    }

    nnz = sparseRep.size();
    if (nnz > threshold * (endCol - startCol))
      sparseToDense();

    buf.readerIndex(buf.readerIndex() + size * 4);
  }

  private void initSparseRep(ByteBuf buf, int size) {
    ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 4);
    initSparseRep(nnz);
    for (int i = 0; i < size; i++) {
      sparseRep.put(buf.readInt(), valueBuf.readInt());
    }
    nnz = sparseRep.size();

    buf.readerIndex(buf.readerIndex() + size * 4);
  }

  public void updateIntDense(ByteBuf buf) {
    int size = buf.readInt();
    if (sparseRep == null && denseRep == null) {
      initDenseRep(buf, size);
    } else if (denseRep != null) {
      densePlusDense(buf, size);
    } else {
      sparsePlusDense(buf, size);
    }
  }

  private void sparsePlusDense(ByteBuf buf, int size) {
    initDenseRep(buf, size);

    int ov, k, v;
    ObjectIterator<Entry> iter = sparseRep.int2IntEntrySet().fastIterator();
    while (iter.hasNext()) {
      Entry itemEntry = iter.next();
      k = itemEntry.getIntKey();
      ov = denseRep.get(k);
      v = ov + itemEntry.getIntValue();
      denseRep.put(k, v);
      if (ov != 0 && v == 0) {
        nnz--;
      }
    }

    // int[] keys = sparseRep.getKeys();
    // int[] values = sparseRep.getValues();
    // boolean[] used = sparseRep.getUsed();
    // int ov, k, v;
    // for (int i = 0; i < keys.length; i++) {
    // if (used[i]) {
    // k = keys[i];
    // ov = denseRep.get(k);
    // v = ov + values[i];
    // denseRep.put(k, v);
    // if (ov != 0 && v == 0)
    // nnz--;
    // }
    // }
    sparseRep = null;
  }

  private void densePlusDense(ByteBuf buf, int size) {
    nnz = 0;
    int value;
    for (int i = 0; i < size; i++) {
      value = denseRep.get(i) + buf.readInt();
      denseRep.put(i, value);
      if (value != 0) {
        nnz++;
      }
    }
    if (nnz < threshold * size)
      denseToSparse();
  }

  @Override
  public void encode(ByteBuf in, ByteBuf out, int len) {
    if (denseRep == null && sparseRep == null)
      encodeEmpty(in, out, len);
    else if (denseRep != null)
      encodeDense(in, out, len);
    else
      encodeSparse(in, out, len);

  }

  @Override public void reset() {
    if(sparseRep != null) {
      sparseRep.clear();
    }

    if(denseRep != null && buf != null) {
      Arrays.fill(buf, (byte) 0);
    }
  }

  private void encodeSparse(ByteBuf in, ByteBuf out, int len) {
    for (int i = 0; i < len; i++) {
      out.writeInt(sparseRep.get(in.readInt()));
    }
  }

  private void encodeDense(ByteBuf in, ByteBuf out, int len) {
    for (int i = 0; i < len; i++) {
      out.writeInt(denseRep.get(in.readInt()));
    }
  }

  private void encodeEmpty(ByteBuf in, ByteBuf out, int len) {
    for (int i = 0; i < len; i++) {
      in.readInt();
      out.writeInt(0);
    }
  }

  @Override
  public void serialize(ByteBuf buf) {

  }

  @Override
  public void deserialize(ByteBuf buf) {

  }

  public int get(int idx) {
    if (sparseRep != null)
      return sparseRep.get(idx);
    if (denseRep != null)
      return denseRep.get(idx);
    else
      return 0;
  }


  /**
   * Get dense representation.
   *
   * @return the int buffer
   */
  public IntBuffer getDenseRep() {
    return denseRep;
  }

  /**
   * Get sparse representation.
   *
   * @return the int hash map
   */
  public Int2IntOpenHashMap getSparseRep() {
    return sparseRep;
  }
}
