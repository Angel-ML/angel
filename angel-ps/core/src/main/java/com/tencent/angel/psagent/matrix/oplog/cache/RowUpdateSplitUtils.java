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
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.psagent.matrix.oplog.cache.splitter.ISplitter;
import com.tencent.angel.utils.Sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.tencent.angel.utils.StringUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.tencent.angel.utils.HashUtils;

/**
 * Row update split utils
 */
public class RowUpdateSplitUtils {

  protected final static Log LOG = LogFactory.getLog(RowUpdateSplitUtils.class);

  /**
   * Vector name to splitter map
   */
  private static ConcurrentHashMap<String, ISplitter> handlers = new ConcurrentHashMap<>();

  private static ISplitter getHandler(String vecClassName) {
    ISplitter splitter = handlers.get(vecClassName);
    if (splitter == null) {
      String splitterClassName =
          "com.tencent.angel.psagent.matrix.oplog.cache.splitter." + vecClassName + "Splitter";

      try {
        splitter = (ISplitter) Class.forName(splitterClassName).newInstance();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
      splitter = handlers.putIfAbsent(vecClassName, splitter);
      if (splitter == null) {
        splitter = handlers.get(vecClassName);
      }
    }
    return splitter;
  }

  /**
   * Split vector to vector splits by vector partitions in PS
   *
   * @param vector the vector need split
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(Vector vector, List<PartitionKey> parts) {
    return getHandler(vector.getClass().getSimpleName()).split(vector, parts);
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
      double[] values, List<PartitionKey> parts) {
    return split(rowId, indices, values, parts, false);
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
      float[] values, List<PartitionKey> parts) {
    return split(rowId, indices, values, parts, false);
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, int[] values,
      List<PartitionKey> parts) {
    return split(rowId, indices, values, parts, false);
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
      double[] values, List<PartitionKey> parts) {
    return split(rowId, indices, values, parts, false);
  }

  /**
   * Split a array to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, double[] values,
      List<PartitionKey> parts) {
    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey part : parts) {
      if (rowId >= part.getStartRow() && rowId < part.getEndRow()) {
        RowUpdateSplit split =
            new DenseDoubleRowUpdateSplit(rowId, (int) part.getStartCol(),
                (int) part.getEndCol(), values);
        ret.put(part, split);
      }
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
      double[] values, List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      int endOffset = (int) parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split =
          new SparseDoubleRowUpdateSplit(rowId, featureIndex - length, featureIndex, indices,
              values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }
    return ret;
  }


  /**
   * Split a array to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] values,
      List<PartitionKey> parts) {
    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey part : parts) {
      if (rowId >= part.getStartRow() && rowId < part.getEndRow()) {
        RowUpdateSplit split = new DenseIntRowUpdateSplit(rowId, (int) part.getStartCol(),
            (int) part.getEndCol(), values);
        ret.put(part, split);
      }
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, int[] values,
      List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      int endOffset = (int) parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split = new SparseIntRowUpdateSplit(rowId, featureIndex - length, featureIndex,
          indices, values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }

    return ret;
  }

  public static class IntIndicesView {
    private final int [] indices;
    private final int start;
    private final int end;

    public IntIndicesView(int [] indices, int start, int end) {
      this.indices = indices;
      this.start = start;
      this.end = end;
    }

    public int[] getIndices() {
      return indices;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }
  }

  public static class LongIndicesView {
    private final long [] indices;
    private final int start;
    private final int end;

    public LongIndicesView(long [] indices, int start, int end) {
      this.indices = indices;
      this.start = start;
      this.end = end;
    }

    public long[] getIndices() {
      return indices;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }
  }

  public static class HashIndicesView {
    private final int [] intIndices;
    private final long [] longIndices;
    private final String [] strIndices;

    public HashIndicesView(int [] intIndices, long [] longIndices, String [] strIndices) {
      this.intIndices = intIndices;
      this.longIndices = longIndices;
      this.strIndices = strIndices;
    }

    public HashIndicesView(int [] intIndices) {
      this(intIndices, null, null);
    }

    public HashIndicesView(long [] longIndices) {
      this(null, longIndices, null);
    }

    public HashIndicesView(String [] strIndices) {
      this(null, null, strIndices);
    }

    public int[] getIntIndices() {
      return intIndices;
    }

    public long[] getLongIndices() {
      return longIndices;
    }

    public String[] getStrIndices() {
      return strIndices;
    }

  }

  public static Map<PartitionKey, IntIndicesView> split(int[] indices, List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Arrays.sort(indices);
    }
    Map<PartitionKey, IntIndicesView> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;

      int endOffset = (int) parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      if(length > 0) {
        IntIndicesView split = new IntIndicesView(indices, featureIndex - length, featureIndex);
        ret.put(parts.get(partIndex), split);
      }

      partIndex++;
    }

    return ret;
  }

  public static Map<PartitionKey, LongIndicesView> split(long[] indices,
                                                         List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Arrays.sort(indices);
    }
    Map<PartitionKey, LongIndicesView> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;

      int endOffset = (int) parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      if(length > 0) {
        LongIndicesView split = new LongIndicesView(indices, featureIndex - length, featureIndex);
        ret.put(parts.get(partIndex), split);
      }

      partIndex++;
    }

    return ret;
  }

  public static Map<PartitionKey, HashIndicesView> split(String[] indices, List<PartitionKey> parts) {
    Map<PartitionKey, HashIndicesView> ret = new HashMap<>();
    Map<Integer, ArrayList<String>> partIndex2Indices = new HashMap<>();
    SortedMap<Integer, Integer> key2partIndex = new TreeMap<>();
    for (int i=0; i<parts.size(); i++) {
      key2partIndex.put((int)parts.get(i).getEndCol(), parts.get(i).getPartitionId());
    }
    for (int j=0; j<indices.length; j++) {
      int hash = HashUtils.getFNV1_32_HashCode(indices[j]);
      SortedMap<Integer, Integer> subMap = key2partIndex.tailMap(hash);
      int key = subMap.firstKey();
      int partIndex = subMap.get(key);
      if (partIndex2Indices.containsKey(partIndex)) {
        partIndex2Indices.get(partIndex).add(indices[j]);
      } else {
        ArrayList<String> splitIndices = new ArrayList<>();
        splitIndices.add(indices[j]);
        partIndex2Indices.put(partIndex, splitIndices);
      }
    }

    for (Map.Entry<Integer, ArrayList<String>> entry : partIndex2Indices.entrySet()) {
      ret.put(parts.get(entry.getKey()), new HashIndicesView(entry.getValue().toArray(new String[0])));
    }

    return ret;
  }

  public static Map<PartitionKey, HashIndicesView> split(int[] indices, List<PartitionKey> parts) {
    Map<PartitionKey, HashIndicesView> ret = new HashMap<>();
    Map<Integer, IntArrayList> partIndex2Indices = new HashMap<>();
    SortedMap<Integer, Integer> key2partIndex = new TreeMap<>();
    for (int i=0; i<parts.size(); i++) {
      key2partIndex.put((int)parts.get(i).getEndCol(), parts.get(i).getPartitionId());
    }
    for (int j=0; j<indices.length; j++) {
      int hash = HashUtils.getFNV1_32_HashCode(indices[j]);
      SortedMap<Integer, Integer> subMap = key2partIndex.tailMap(hash);
      int key = subMap.firstKey();
      int partIndex = subMap.get(key);
      if (partIndex2Indices.containsKey(partIndex)) {
        partIndex2Indices.get(partIndex).add(indices[j]);
      } else {
        IntArrayList splitIndices = new IntArrayList();
        splitIndices.add(indices[j]);
        partIndex2Indices.put(partIndex, splitIndices);
      }
    }

    for (Map.Entry<Integer, IntArrayList> entry : partIndex2Indices.entrySet()) {
      ret.put(parts.get(entry.getKey()), new HashIndicesView(entry.getValue().toIntArray()));
    }

    return ret;
  }

  public static Map<PartitionKey, HashIndicesView> split(long[] indices, List<PartitionKey> parts) {
    Map<PartitionKey, HashIndicesView> ret = new HashMap<>();
    Map<Integer, LongArrayList> partIndex2Indices = new HashMap<>();
    SortedMap<Integer, Integer> key2partIndex = new TreeMap<>();
    for (int i=0; i<parts.size(); i++) {
      key2partIndex.put((int)parts.get(i).getEndCol(), parts.get(i).getPartitionId());
    }
    for (int j=0; j<indices.length; j++) {
      int hash = HashUtils.getFNV1_32_HashCode(indices[j]);
      SortedMap<Integer, Integer> subMap = key2partIndex.tailMap(hash);
      int key = subMap.firstKey();
      int partIndex = subMap.get(key);
      if (partIndex2Indices.containsKey(partIndex)) {
        partIndex2Indices.get(partIndex).add(indices[j]);
      } else {
        LongArrayList splitIndices = new LongArrayList();
        splitIndices.add(indices[j]);
        partIndex2Indices.put(partIndex, splitIndices);
      }
    }

    for (Map.Entry<Integer, LongArrayList> entry : partIndex2Indices.entrySet()) {
      ret.put(parts.get(entry.getKey()), new HashIndicesView(entry.getValue().toLongArray()));
    }

    return ret;
  }

  /**
   * Split a array to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, long[] values,
      List<PartitionKey> parts) {
    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey part : parts) {
      if (rowId >= part.getStartRow() && rowId < part.getEndRow()) {
        RowUpdateSplit split = new DenseLongRowUpdateSplit(rowId, (int) part.getStartCol(),
            (int) part.getEndCol(), values);
        ret.put(part, split);
      }
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices, long[] values,
      List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      int endOffset = (int) parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split = new SparseLongRowUpdateSplit(rowId, featureIndex - length,
          featureIndex, indices, values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }

    return ret;
  }

  /**
   * Split a array to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, float[] values,
      List<PartitionKey> parts) {
    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();
    for (PartitionKey part : parts) {
      if (rowId >= part.getStartRow() && rowId < part.getEndRow()) {
        RowUpdateSplit split = new DenseFloatRowUpdateSplit(rowId, (int) part.getStartCol(),
            (int) part.getEndCol(), values);
        ret.put(part, split);
      }
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, int[] indices,
      float[] values, List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      int endOffset = (int) parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split = new SparseFloatRowUpdateSplit(rowId, featureIndex - length,
          featureIndex, indices, values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
      double[] values, List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split =
          new LongKeySparseDoubleRowUpdateSplit(rowId, featureIndex - length, featureIndex, indices,
              values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices, int[] values,
      List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split =
          new LongKeySparseIntRowUpdateSplit(rowId, featureIndex - length, featureIndex, indices,
              values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
      long[] values, List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split =
          new LongKeySparseLongRowUpdateSplit(rowId, featureIndex - length, featureIndex, indices,
              values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }
    return ret;
  }

  /**
   * Split a array pair to vector splits by vector partitions in PS
   *
   * @param rowId need update row id
   * @param indices column indices
   * @param values column values, the dimension must equals to indices
   * @param parts partitions that contain this vector, this partition must be sorted by part start
   * column index
   * @param sorted true means sort the indices and values first
   * @return part to split map
   */
  public static Map<PartitionKey, RowUpdateSplit> split(int rowId, long[] indices,
      float[] values, List<PartitionKey> parts, boolean sorted) {
    if (!sorted) {
      Sort.quickSort(indices, values, 0, indices.length - 1);
    }

    Map<PartitionKey, RowUpdateSplit> ret = new HashMap<>();

    int featureIndex = 0;
    int partIndex = 0;

    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (featureIndex < indices.length || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (featureIndex < indices.length && indices[featureIndex] < endOffset) {
        featureIndex++;
        length++;
      }

      RowUpdateSplit split =
          new LongKeySparseFloatRowUpdateSplit(rowId, featureIndex - length, featureIndex, indices,
              values);
      ret.put(parts.get(partIndex), split);

      partIndex++;
    }
    return ret;
  }


  public static boolean isInRange(int[] sortedIndices, List<PartitionKey> sortedParts) {
    if (sortedIndices == null || sortedIndices.length == 0) {
      return true;
    }

    return sortedIndices[0] >= sortedParts.get(0).getStartCol()
        && sortedIndices[sortedIndices.length - 1] < sortedParts.get(sortedParts.size() - 1)
        .getEndCol();
  }

  public static boolean isInRange(long[] sortedIndices, List<PartitionKey> sortedParts) {
    if (sortedIndices == null || sortedIndices.length == 0) {
      return true;
    }

    return sortedIndices[0] >= sortedParts.get(0).getStartCol()
        && sortedIndices[sortedIndices.length - 1] < sortedParts.get(sortedParts.size() - 1)
        .getEndCol();
  }

  public static boolean isInRange(long[] indices, int[] sortedIndex,
                                  List<PartitionKey> sortedParts) {
    if (indices == null || indices.length == 0)
      return true;

    return indices[sortedIndex[0]] >= sortedParts.get(0).getStartCol()
      && indices[sortedIndex[sortedIndex.length - 1]] < sortedParts.get(sortedParts.size() - 1)
      .getEndCol();
  }
}
