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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Matrix partitions router table.
 */
public class MatrixPartitionRouter {
  private static final Log LOG = LogFactory.getLog(MatrixPartitionRouter.class);

  /** matrix partition key to server that holds the partition map */
  private final ConcurrentHashMap<PartitionKey, ParameterServerId> partitionPSIndex;

  /** matrix id to the partitions of the matrix map */
  private final ConcurrentHashMap<Integer, List<PartitionKey>> matrixToPartitionList;

  /**
   * Create a new MatrixPartitionRouter.
   *
   * @param partitionPSIndex the router information of matrix partitions
   */
  public MatrixPartitionRouter(HashMap<PartitionKey, ParameterServerId> partitionPSIndex) {
    this.partitionPSIndex = new ConcurrentHashMap<PartitionKey, ParameterServerId>();
    this.matrixToPartitionList = new ConcurrentHashMap<Integer, List<PartitionKey>>();
    this.partitionPSIndex.putAll(partitionPSIndex);
    for (Entry<PartitionKey, ParameterServerId> entry : partitionPSIndex.entrySet()) {
      List<PartitionKey> partList = matrixToPartitionList.get(entry.getKey().getMatrixId());
      if (partList == null) {
        partList = new ArrayList<PartitionKey>();
        matrixToPartitionList.put(entry.getKey().getMatrixId(), partList);
      }
      partList.add(entry.getKey());
      LOG.debug("add partition " + entry.getKey() + " for matrix " + entry.getKey().getMatrixId());
    }
  }

  /**
   * Get the server that hold the partition.
   * 
   * @param partitionKey matrix partition key
   * @return ParameterServerId server id
   */
  public ParameterServerId getPSId(PartitionKey partitionKey) {
    return partitionPSIndex.get(partitionKey);
  }

  /**
   * Get list of partitionKeys for matrixId and rowId.
   * 
   * @param matrixId, matrixId
   * @param rowId, rowId
   * @return list of partitionKeys
   */
  public List<PartitionKey> getPartitionKeyList(int matrixId, int rowId) {
    List<PartitionKey> partitionKeyList = new ArrayList<>();
    Iterator<PartitionKey> iter = partitionPSIndex.keySet().iterator();
    while (iter.hasNext()) {
      PartitionKey partitionKey = iter.next();
      if (partitionKey.getMatrixId() == matrixId && partitionKey.getStartRow() <= rowId
          && partitionKey.getEndRow() > rowId)
        partitionKeyList.add(partitionKey);
    }
    return partitionKeyList;
  }

  /**
   * Get list of partitionkeys belong to matrixId.
   * 
   * @param matrixId, identity number of matrix requested
   * @return list of partitionKeys
   */
  public List<PartitionKey> getPartitionKeyList(int matrixId) {
    List<PartitionKey> partitionKeyList = matrixToPartitionList.get(matrixId);
    if (partitionKeyList == null) {
      partitionKeyList = new ArrayList<>();
      Iterator<PartitionKey> iter = partitionPSIndex.keySet().iterator();
      while (iter.hasNext()) {
        PartitionKey partitionKey = iter.next();
        if (partitionKey.getMatrixId() == matrixId)
          partitionKeyList.add(partitionKey);
      }
      matrixToPartitionList.putIfAbsent(matrixId, partitionKeyList);
      partitionKeyList = matrixToPartitionList.get(matrixId);
    }

    return partitionKeyList;
  }

  /**
   * Get the partitions the rows in.
   * 
   * @param matrixId matrix id
   * @param rowIndexes row indexes
   * @return Map<PartitionKey, List<Integer>> partitions to the rows contained in the partition map
   */
  public Map<PartitionKey, List<Integer>> getPartitionKeyRowIndexesMap(int matrixId,
      int[] rowIndexes) {
    Map<PartitionKey, List<Integer>> partToRowIndexesMap =
        new HashMap<PartitionKey, List<Integer>>();
    for (int i = 0; i < rowIndexes.length; i++) {
      Iterator<PartitionKey> iter = partitionPSIndex.keySet().iterator();
      while (iter.hasNext()) {
        PartitionKey partitionKey = iter.next();
        if (partitionKey.getMatrixId() == matrixId && partitionKey.getStartRow() <= rowIndexes[i]
            && partitionKey.getEndRow() > rowIndexes[i]) {

          List<Integer> indexList = partToRowIndexesMap.get(partitionKey);
          if (indexList == null) {
            indexList = new ArrayList<Integer>();
            partToRowIndexesMap.put(partitionKey, indexList);           
          }
          indexList.add(rowIndexes[i]);
        }
      }
    }

    return partToRowIndexesMap;
  }
  
  /**
   * Get the partitions the rows in.
   * 
   * @param matrixId matrix id
   * @param rowIndexes row indexes
   * @return Map<PartitionKey, List<Integer>> partitions to the rows contained in the partition map
   */
  public Map<PartitionKey, List<Integer>> getPartitionKeyRowIndexesMap(int matrixId,
      List<Integer> rowIndexes) {
    Map<PartitionKey, List<Integer>> partToRowIndexesMap =
        new HashMap<PartitionKey, List<Integer>>();
    int size = rowIndexes.size();
    for (int i = 0; i < size; i++) {
      Iterator<PartitionKey> iter = partitionPSIndex.keySet().iterator();
      while (iter.hasNext()) {
        PartitionKey partitionKey = iter.next();
        if (partitionKey.getMatrixId() == matrixId && partitionKey.getStartRow() <= rowIndexes.get(i)
            && partitionKey.getEndRow() > rowIndexes.get(i)) {

          List<Integer> indexList = partToRowIndexesMap.get(partitionKey);
          if (indexList == null) {
            indexList = new ArrayList<Integer>();
            partToRowIndexesMap.put(partitionKey, indexList);            
          }
          
          indexList.add(rowIndexes.get(i));
        }
      }
    }

    return partToRowIndexesMap;
  }

  /**
   * Get the partitions the rows in.
   * 
   * @param rowIndex matrix id and row indexes
   * @param batchNumber the split batch size.
   * @return Map<PartitionKey, List<Integer>> partitions to the rows contained in the partition map
   */
  public Map<PartitionKey, List<RowIndex>> getPartitionKeyRowIndexMap(RowIndex rowIndex,
      int batchNumber) {
    Map<PartitionKey, List<RowIndex>> partToRowIndexMap =
        new TreeMap<PartitionKey, List<RowIndex>>();
    if (rowIndex.getRowIds() == null) {
      return partToRowIndexMap;
    }

    IntOpenHashSet rowIdSet = rowIndex.getRowIds();
    IntOpenHashSet filtedRowIdSet = rowIndex.getFiltedIdSet();
    int[] rowIds = new int[rowIdSet.size() - filtedRowIdSet.size()];
    int count = 0;
    for (int rowId : rowIdSet) {
      if (!filtedRowIdSet.contains(rowId)) {
        rowIds[count++] = rowId;
      }
    }

    Arrays.sort(rowIds);

    for (int i = 0; i < rowIds.length; i++) {
      Iterator<PartitionKey> iter = partitionPSIndex.keySet().iterator();
      while (iter.hasNext()) {
        PartitionKey partitionKey = iter.next();
        if (partitionKey.getMatrixId() == rowIndex.getMatrixId()
            && partitionKey.getStartRow() <= rowIds[i] && partitionKey.getEndRow() > rowIds[i]) {

          List<RowIndex> indexList = partToRowIndexMap.get(partitionKey);
          if (indexList == null) {
            indexList = new ArrayList<RowIndex>();
            partToRowIndexMap.put(partitionKey, indexList);
            indexList.add(new RowIndex(rowIndex.getMatrixId(), rowIndex));
          }

          RowIndex index = indexList.get(indexList.size() - 1);
          if (index.getRowsNumber() >= batchNumber) {
            index = new RowIndex(rowIndex.getMatrixId(), rowIndex);
            indexList.add(index);
          }
          index.addRowId(rowIds[i]);
        }
      }
    }

    return partToRowIndexMap;
  }

  /**
   * Get the number of the partitions that contains the row.
   * 
   * @param matrixId matrix id
   * @param rowId row index
   * @return the number of the partitions that contains the row
   */
  public int getRowPartitionSize(int matrixId, int rowId) {
    int ret = 0;
    for (PartitionKey partitionKey : partitionPSIndex.keySet()) {
      if (partitionKey.getMatrixId() == matrixId && partitionKey.getStartRow() <= rowId
          && partitionKey.getEndRow() > rowId) {
        ret++;
      }
    }
    return ret;
  }

  /**
   * Put a matrix partition router item to the router table.
   * 
   * @param partitionKey matrix partition key
   * @param parameterServerId server id
   */
  public void addPartition(PartitionKey partitionKey, ParameterServerId parameterServerId) {
    partitionPSIndex.putIfAbsent(partitionKey, parameterServerId);
  }

  /**
   * Get the matrix id to the partitions of the matrix map.
   * 
   * @return Map<Integer, List<PartitionKey>> the matrix id to the partitions of the matrix map
   */
  public Map<Integer, List<PartitionKey>> getMatrixToPartMap() {
    return matrixToPartitionList;
  }

  /**
   * Clear the router table.
   */
  public void clear() {
    partitionPSIndex.clear();
    matrixToPartitionList.clear();
  }
}
