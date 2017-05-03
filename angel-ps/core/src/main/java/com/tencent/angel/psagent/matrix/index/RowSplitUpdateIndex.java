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

package com.tencent.angel.psagent.matrix.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.ByteString;
import com.tencent.angel.psagent.matrix.oplog.cache.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RowSplitUpdateIndex {
  protected final static Log LOG = LogFactory.getLog(RowSplitUpdateIndex.class);
  private int[] indexes;
  private final Set<Integer> indexSet;
  private final Lock readLock;
  private final Lock writeLock;
  private ByteString encodedIndexes;

  public RowSplitUpdateIndex() {
    indexSet = new HashSet<Integer>();
    ReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public int[] getIndexes() {
    return indexes;
  }

  public void setIndexes(int[] indexes) {
    this.indexes = indexes;
  }

  public Set<Integer> getIndexSet() {
    return indexSet;
  }

  public Lock getReadLock() {
    return readLock;
  }

  public Lock getWriteLock() {
    return writeLock;
  }

  public void addUpdateIndexes(RowUpdateSplit dataUpdateSplit) {
    if (dataUpdateSplit instanceof DenseDoubleRowUpdateSplit) {
      addUpdateIndexes((DenseDoubleRowUpdateSplit) dataUpdateSplit);
    } else if (dataUpdateSplit instanceof DenseIntRowUpdateSplit) {
      addUpdateIndexes((DenseIntRowUpdateSplit) dataUpdateSplit);
    } else if (dataUpdateSplit instanceof SparseDoubleRowUpdateSplit) {
      addUpdateIndexes((SparseDoubleRowUpdateSplit) dataUpdateSplit);
    } else if (dataUpdateSplit instanceof DenseFloatRowUpdateSplit) {
      addUpdateIndexes((DenseFloatRowUpdateSplit) dataUpdateSplit);
    } else if (dataUpdateSplit instanceof SparseFloatRowUpdateSplit) {
      addUpdateIndexes((SparseFloatRowUpdateSplit) dataUpdateSplit);
    }
    else {
      LOG.error("unkown dataupdatesplit type " + dataUpdateSplit.getClass().getName());
    }
  }

  private void addUpdateIndexes(DenseDoubleRowUpdateSplit dataUpdateSplit) {
    double[] values = dataUpdateSplit.getValues();
    try {
      writeLock.lock();
      int start = dataUpdateSplit.getStart();
      int len = dataUpdateSplit.getEnd() - start;
      for (int i = 0; i < len && i < values.length; i++) {
        if (values[i + start] != 0.0) {
          indexSet.add(i);
        }
      }

      LOG.debug("after update, index number is " + indexSet.size());
    } finally {
      writeLock.unlock();
    }
  }

  private void addUpdateIndexes(DenseFloatRowUpdateSplit dataUpdateSplit) {
    float[] values = dataUpdateSplit.getValues();
    try {
      writeLock.lock();
      int start = dataUpdateSplit.getStart();
      int len = dataUpdateSplit.getEnd() - start;
      for (int i = 0; i < len && i < values.length; i++) {
        if (values[i + start] != 0.0) {
          indexSet.add(i);
        }
      }

      LOG.debug("after update, index number is " + indexSet.size());
    } finally {
      writeLock.unlock();
    }
  }

  private void addUpdateIndexes(DenseIntRowUpdateSplit dataUpdateSplit) {
    int[] values = dataUpdateSplit.getValues();
    try {
      writeLock.lock();
      int start = dataUpdateSplit.getStart();
      int len = dataUpdateSplit.getEnd() - start;
      for (int i = 0; i < len && i < values.length; i++) {
        if (values[i + start] != 0) {
          indexSet.add(i);
        }
      }

      LOG.debug("after update, index number is " + indexSet.size());
    } finally {
      writeLock.unlock();
    }
  }

  private void addUpdateIndexes(SparseDoubleRowUpdateSplit dataUpdateSplit) {
    int[] indexes = dataUpdateSplit.getOffsets();
    int startPos = dataUpdateSplit.getStart();
    int endPos = dataUpdateSplit.getEnd();
    if (startPos < 0) {
      LOG.info("there are no updates for range(" + dataUpdateSplit.getStart() + ","
          + dataUpdateSplit.getEnd());
    } else {
      try {
        writeLock.lock();
        for (int i = startPos; i < endPos; i++) {
          indexSet.add(indexes[i]);
        }

        LOG.debug("after update, index number is " + indexSet.size());
      } finally {
        writeLock.unlock();
      }

    }
  }

  private void addUpdateIndexes(SparseFloatRowUpdateSplit dataUpdateSplit) {
    int[] indexes = dataUpdateSplit.getOffsets();
    int startPos = dataUpdateSplit.getStart();
    int endPos = dataUpdateSplit.getEnd();
    if (startPos < 0) {
      LOG.info("there are no updates for range(" + dataUpdateSplit.getStart() + ","
          + dataUpdateSplit.getEnd());
    } else {
      try {
        writeLock.lock();
        for (int i = startPos; i < endPos; i++) {
          indexSet.add(indexes[i]);
        }

        LOG.debug("after update, index number is " + indexSet.size());
      } finally {
        writeLock.unlock();
      }

    }
  }


  private int findStartPos(int[] indexes, int pos) {
    return findStartPos(indexes, pos, 0, indexes.length - 1);
  }

  private int findStartPos(int[] indexes, int pos, int startPos, int endPos) {
    if (startPos > endPos) {
      return -1;
    }

    int mid = (startPos + endPos) / 2;
    if (mid < 1) {
      return 0;
    }

    if (indexes[mid] >= pos && indexes[mid - 1] < pos) {
      return mid;
    } else if (indexes[mid] < pos) {
      return findStartPos(indexes, pos, mid + 1, endPos);
    } else {
      return findStartPos(indexes, pos, startPos, mid - 1);
    }
  }

  public ByteString getSerilizedIndexes() {
    if (encodedIndexes != null) {
      return encodedIndexes;
    } else {
      try {
        readLock.lock();
        indexes = new int[indexSet.size()];

        Output output = new Output((indexSet.size() + 1) * 4, Integer.MAX_VALUE);
        Kryo kryo = new Kryo();
        int pos = 0;
        kryo.register(Integer.class);
        kryo.writeObject(output, indexSet.size());
        for (Integer index : indexSet) {
          kryo.writeObject(output, index);
          indexes[pos++] = index;
        }
        indexSet.clear();
        encodedIndexes = ByteString.copyFrom(output.getBuffer(), 0, output.position());
      } finally {
        readLock.unlock();
      }

      return encodedIndexes;
    }
  }
}
