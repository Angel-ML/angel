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

package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.utils.Sort;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class RowUpdateSplitUtils {
  protected final static Log LOG = LogFactory.getLog(RowUpdateSplitUtils.class);

  public static HashMap<PartitionKey, RowUpdateSplit> split(DenseDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    LOG.debug("Split a dense vector into multiple splits according to partition information");
    return split(vector.getRowId(), vector.getValues(), partitionInfos);
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

  public static HashMap<PartitionKey, RowUpdateSplit> split(DenseIntVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getValues(), partitionInfos);
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

  public static HashMap<PartitionKey, RowUpdateSplit> split(DenseFloatVector vector,
    List<PartitionKey> partitionInfos) {
    LOG.debug(
      "Split a dense float vector into multiple splits according to partition " + "information");
    return split(vector.getRowId(), vector.getValues(), partitionInfos);
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

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
    double[] values, List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
    float[] values, List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
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

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

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

    Collections.sort(partitionOfVector, new Comparator<PartitionKey>() {
      @Override public int compare(PartitionKey key1, PartitionKey key2) {
        return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
      }
    });

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

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, int[] values,
    List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
    double[] values, List<PartitionKey> partitionInfos) {
    return split(rowId, indices, values, partitionInfos, false);
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

  public static HashMap<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
    double[] values, List<PartitionKey> partitionInfos, boolean sorted) {

    long startTs = System.currentTimeMillis();
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }
    LOG.info("sort use time=" + (System.currentTimeMillis() - startTs));

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
    startTs = System.currentTimeMillis();
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
        new LongKeySparseDoubleRowUpdateSplit(rowId, MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY,
          ii - length, ii, indices, values);
      ret.put(partitionOfVector.get(keyIndex), split);

      keyIndex++;
    }
    LOG.info("split use time=" + (System.currentTimeMillis() - startTs));
    return ret;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(SparseDoubleSortedVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getIndices(), vector.getValues(), partitionInfos, true);
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(SparseDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getIndices(), vector.getValues(), partitionInfos);
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(SparseFloatVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getIndices(), vector.getValues(), partitionInfos);
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(SparseIntVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getIndices(), vector.getValues(), partitionInfos);
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(SparseDoubleLongKeyVector vector,
    List<PartitionKey> partitionInfos) {
    return split(vector.getRowId(), vector.getIndexes(), vector.getValues(), partitionInfos);
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompSparseDoubleLongKeyVector vector,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    SparseDoubleLongKeyVector[] splits = vector.getSplits();
    PartitionKey[] partKeys = vector.getPartKeys();
    for(int i = 0; i < partKeys.length; i++) {
      if(partKeys[i] != null && splits[i] != null) {
        updateSplitMap.put(partKeys[i], new CompSparseDoubleLongKeyRowUpdateSplit(splits[i],
          vector.getRowId(), MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY));
      }
    }

    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompSparseDoubleVector vector,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    TDoubleVector[] splits = vector.getVectors();
    PartitionKey[] partKeys = vector.getPartKeys();
    for(int i = 0; i < partKeys.length; i++) {
      if(partKeys[i] != null && splits[i] != null) {
        updateSplitMap.put(partKeys[i], new CompSparseDoubleRowUpdateSplit((SparseDoubleVector)splits[i],
          vector.getRowId(), MLProtos.RowType.T_DOUBLE_SPARSE));
      }
    }

    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompSparseFloatVector vector,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    TFloatVector[] splits = vector.getVectors();
    PartitionKey[] partKeys = vector.getPartKeys();
    for(int i = 0; i < partKeys.length; i++) {
      if(partKeys[i] != null && splits[i] != null) {
        updateSplitMap.put(partKeys[i], new CompSparseFloatRowUpdateSplit((SparseFloatVector)splits[i],
          vector.getRowId(), MLProtos.RowType.T_FLOAT_SPARSE));
      }
    }

    return updateSplitMap;
  }

  private static HashMap<PartitionKey, RowUpdateSplit> split(CompSparseIntVector vector,
    List<PartitionKey> partitionInfos) {
    HashMap<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(partitionInfos.size());
    TIntVector[] splits = vector.getVectors();
    PartitionKey[] partKeys = vector.getPartKeys();
    for(int i = 0; i < partKeys.length; i++) {
      if(partKeys[i] != null && splits[i] != null) {
        updateSplitMap.put(partKeys[i], new CompSparseIntRowUpdateSplit((SparseIntVector)splits[i],
          vector.getRowId(), MLProtos.RowType.T_INT_SPARSE));
      }
    }

    return updateSplitMap;
  }

  public static HashMap<PartitionKey, RowUpdateSplit> split(TVector vector,
    List<PartitionKey> partitionInfos) {
    if (vector instanceof DenseDoubleVector)
      return split((DenseDoubleVector) vector, partitionInfos);

    if (vector instanceof SparseDoubleVector)
      return split((SparseDoubleVector) vector, partitionInfos);

    if (vector instanceof SparseDoubleSortedVector)
      return split((SparseDoubleSortedVector) vector, partitionInfos);

    if (vector instanceof DenseIntVector)
      return split((DenseIntVector) vector, partitionInfos);

    if (vector instanceof SparseIntVector)
      return split((SparseIntVector) vector, partitionInfos);

    if (vector instanceof DenseFloatVector)
      return split((DenseFloatVector) vector, partitionInfos);

    if (vector instanceof SparseFloatVector)
      return split((SparseFloatVector) vector, partitionInfos);

    if (vector instanceof SparseDoubleLongKeyVector)
      return split((SparseDoubleLongKeyVector) vector, partitionInfos);

    if (vector instanceof CompSparseDoubleLongKeyVector)
      return split((CompSparseDoubleLongKeyVector) vector, partitionInfos);

    if (vector instanceof CompSparseDoubleVector)
      return split((CompSparseDoubleVector) vector, partitionInfos);

    if (vector instanceof CompSparseFloatVector)
      return split((CompSparseFloatVector) vector, partitionInfos);

    if (vector instanceof CompSparseIntVector)
      return split((CompSparseIntVector) vector, partitionInfos);

    throw new UnsupportedOperationException(
      "Unsupport operation: split " + vector.getClass().getName());
  }
}
