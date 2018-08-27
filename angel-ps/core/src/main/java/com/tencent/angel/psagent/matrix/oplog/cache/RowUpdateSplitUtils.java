/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.utils.Sort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class RowUpdateSplitUtils {
  protected final static Log LOG = LogFactory.getLog(RowUpdateSplitUtils.class);


  static class PartitionComp implements Comparator<PartitionKey> {
    @Override public int compare(PartitionKey key1, PartitionKey key2) {
      return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
    double[] values, List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
    float[] values, List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, int[] values,
    List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
    double[] values, List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(Vector vector,
    List<PartitionKey> partitionInfos) {
    if (vector instanceof IntDoubleVector)
      return split((IntDoubleVector) vector, partitionInfos);

    if (vector instanceof IntIntVector)
      return split((IntIntVector) vector, partitionInfos);

    if (vector instanceof IntLongVector)
      return split((IntLongVector) vector, partitionInfos);

    if (vector instanceof IntFloatVector)
      return split((IntFloatVector) vector, partitionInfos);

    if (vector instanceof LongDoubleVector)
      return split((LongDoubleVector) vector, partitionInfos);

    if (vector instanceof LongIntVector)
      return split((LongIntVector) vector, partitionInfos);

    if (vector instanceof LongLongVector)
      return split((LongLongVector) vector, partitionInfos);

    if (vector instanceof LongFloatVector)
      return split((LongFloatVector) vector, partitionInfos);

    if (vector instanceof CompIntDoubleVector)
      return split((CompIntDoubleVector) vector, partitionInfos);

    if (vector instanceof CompLongDoubleVector)
      return split((CompLongDoubleVector) vector, partitionInfos);

    if (vector instanceof CompIntFloatVector)
      return split((CompIntFloatVector) vector, partitionInfos);

    if (vector instanceof CompLongFloatVector)
      return split((CompLongFloatVector) vector, partitionInfos);

    if (vector instanceof CompIntIntVector)
      return split((CompIntIntVector) vector, partitionInfos);

    if (vector instanceof CompLongIntVector)
      return split((CompLongIntVector) vector, partitionInfos);

    if (vector instanceof CompIntLongVector)
      return split((CompIntLongVector) vector, partitionInfos);

    if (vector instanceof CompLongLongVector)
      return split((CompLongLongVector) vector, partitionInfos);

    throw new UnsupportedOperationException(
      "Unsupport operation: split " + vector.getClass().getName());
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(IntDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId,
    IntDoubleVectorStorage storage, List<PartitionKey> partitionInfos) {
    if (storage instanceof IntDoubleDenseVectorStorage) {
      return split(rowId, storage.getValues(), partitionInfos);
    } else if (storage instanceof IntDoubleSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof IntDoubleSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, double[] values,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        RowUpdateSplit split =
          new DenseDoubleRowUpdateSplit(rowId, (int) partitionKey.getStartCol(),
            (int) partitionKey.getEndCol(), values);
        ret.put(partitionKey, split);
      }
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
    double[] values, List<PartitionKey> partitionInfos, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    LOG.debug("split sparse double vector, rowId=" + rowId);
    for (PartitionKey partitionKey : partitionInfos) {
      LOG.debug("split sparse double vector, rowId=" + rowId + ", partitionKey.getStartRow()="
        + partitionKey.getStartRow() + ", partitionKey.getEndRow()=" + partitionKey.getEndRow());
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new PartitionComp());

    int ii = 0;
    int keyIndex = 0;
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      int endOffset = (int) partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split =
        new SparseDoubleRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    return ret;
  }


  public static HashMap<PartitionKey, RowUpdateSplit> split(IntIntVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, IntIntVectorStorage storage,
    List<PartitionKey> partitionInfos) {
    if (storage instanceof IntIntDenseVectorStorage) {
      return split(rowId, storage.getValues(), partitionInfos);
    } else if (storage instanceof IntIntSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof IntIntSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] values,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        RowUpdateSplit split = new DenseIntRowUpdateSplit(rowId, (int) partitionKey.getStartCol(),
          (int) partitionKey.getEndCol(), values);
        ret.put(partitionKey, split);
      }
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, int[] values,
    List<PartitionKey> partitionInfos, boolean sorted) {

    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

    int ii = 0;
    int keyIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      int endOffset = (int) partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split = new SparseIntRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }

    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(IntLongVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, IntLongVectorStorage storage,
    List<PartitionKey> partitionInfos) {
    if (storage instanceof IntLongDenseVectorStorage) {
      return split(rowId, storage.getValues(), partitionInfos);
    } else if (storage instanceof IntLongSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof IntLongSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] values,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        RowUpdateSplit split = new DenseLongRowUpdateSplit(rowId, (int) partitionKey.getStartCol(),
          (int) partitionKey.getEndCol(), values);
        ret.put(partitionKey, split);
      }
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, long[] values,
    List<PartitionKey> partitionInfos, boolean sorted) {

    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

    int ii = 0;
    int keyIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      int endOffset = (int) partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split = new SparseLongRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }

    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(IntFloatVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId,
    IntFloatVectorStorage storage, List<PartitionKey> partitionInfos) {
    if (storage instanceof IntFloatDenseVectorStorage) {
      return split(rowId, storage.getValues(), partitionInfos);
    } else if (storage instanceof IntFloatSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof IntFloatSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, float[] values,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        RowUpdateSplit split = new DenseFloatRowUpdateSplit(rowId, (int) partitionKey.getStartCol(),
          (int) partitionKey.getEndCol(), values);
        ret.put(partitionKey, split);
      }
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
    float[] values, List<PartitionKey> partitionInfos, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new PartitionComp());

    int ii = 0;
    int keyIndex = 0;
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      int endOffset = (int) partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split = new SparseFloatRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(LongDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId,
    LongDoubleVectorStorage storage, List<PartitionKey> partitionInfos) {
    if (storage instanceof LongDoubleSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof LongDoubleSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
    double[] values, List<PartitionKey> partitionInfos, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

    int ii = 0;
    int keyIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      long endOffset = partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split =
        new LongKeySparseDoubleRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(LongIntVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, LongIntVectorStorage storage,
    List<PartitionKey> partitionInfos) {
    if (storage instanceof LongIntSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof LongIntSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices, int[] values,
    List<PartitionKey> partitionInfos, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

    int ii = 0;
    int keyIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      long endOffset = partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split =
        new LongKeySparseIntRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(LongLongVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId,
    LongLongVectorStorage storage, List<PartitionKey> partitionInfos) {
    if (storage instanceof LongLongSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof LongLongSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
    long[] values, List<PartitionKey> partitionInfos, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

    int ii = 0;
    int keyIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      long endOffset = partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split =
        new LongKeySparseLongRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    return ret;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(LongFloatVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getStorage(), partitionInfos);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId,
    LongFloatVectorStorage storage, List<PartitionKey> partitionInfos) {
    if (storage instanceof LongFloatSparseVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, false);
    } else if (storage instanceof LongFloatSortedVectorStorage) {
      return split(rowId, storage.getIndices(), storage.getValues(), partitionInfos, true);
    } else {
      throw new UnsupportedOperationException(
        "unsupport split for storage type:" + storage.getClass().getName());
    }
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
    float[] values, List<PartitionKey> partitionInfos, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    HashMap<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    ArrayList<PartitionKey> partitionOfVector = new ArrayList<>();

    for (PartitionKey partitionKey : partitionInfos) {
      if (rowId >= partitionKey.getStartRow() && rowId < partitionKey.getEndRow()) {
        partitionOfVector.add(partitionKey);
      }
    }

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

    int ii = 0;
    int keyIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indices.length || keyIndex < partitionOfVector.size()) {
      int length = 0;
      long endOffset = partitionOfVector.get(keyIndex).getEndCol();
      while (ii < indices.length && indices[ii] < endOffset) {
        ii++;
        length++;
      }

      RowUpdateSplit split =
        new LongKeySparseFloatRowUpdateSplit(rowId, ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    return ret;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompLongDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    LongDoubleVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap.put(partitionInfos.get(i),
        new CompLongDoubleRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompIntDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    IntDoubleVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap.put(partitionInfos.get(i),
        new CompIntDoubleRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompIntFloatVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    IntFloatVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap
        .put(partitionInfos.get(i), new CompIntFloatRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompLongFloatVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    LongFloatVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap.put(partitionInfos.get(i),
        new CompLongFloatRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompLongIntVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    LongIntVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap
        .put(partitionInfos.get(i), new CompLongIntRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompIntIntVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    IntIntVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap
        .put(partitionInfos.get(i), new CompIntIntRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompIntLongVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    IntLongVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap
        .put(partitionInfos.get(i), new CompIntLongRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompLongLongVector vector,
    List<PartitionKey> partitionInfos) {
    partitionInfos.sort(new PartitionComp());
    LongLongVector[] vecParts = vector.getPartitions();
    assert vecParts.length == partitionInfos.size();

    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap
        .put(partitionInfos.get(i), new CompLongLongRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }
}
