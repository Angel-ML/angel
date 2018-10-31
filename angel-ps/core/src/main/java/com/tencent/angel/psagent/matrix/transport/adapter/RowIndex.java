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


package com.tencent.angel.psagent.matrix.transport.adapter;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.Arrays;
import java.util.UUID;

/**
 * Row indexes for GET_ROWS request.
 */
public class RowIndex {
  /**
   * index set of rows that need to fetch
   */
  private final IntOpenHashSet rowIdSet;

  /**
   * index set of rows that are found in matrix storage or matrix cache
   */
  private final IntOpenHashSet filtedIdSet;

  /**
   * parent RowIndex instance, a RowIndex instance can be splited to many son RowIndex instances
   */
  private final RowIndex parent;

  /**
   * matrix id
   */
  private int matrixId;
  private final UUID uuid;

  /**
   * Create a new RowIndex.
   *
   * @param matrixId matrix id
   * @param rowIdSet need fetch row index set
   * @param parent   parent RowIndex instance
   */
  public RowIndex(int matrixId, IntOpenHashSet rowIdSet, RowIndex parent) {
    this.matrixId = matrixId;
    this.rowIdSet = rowIdSet;
    this.parent = parent;
    this.uuid = UUID.randomUUID();
    this.filtedIdSet = new IntOpenHashSet();
  }

  /**
   * Create a new RowIndex.
   */
  public RowIndex() {
    this(0, new IntOpenHashSet(), null);
  }

  /**
   * Create a new RowIndex.
   */
  public RowIndex(int [] rowIds) {
    this(0, new IntOpenHashSet(rowIds), null);
  }


  /**
   * Create a new RowIndex.
   *
   * @param matrixId matrix id
   * @param parent need fetch row index set
   */
  public RowIndex(int matrixId, RowIndex parent) {
    this(matrixId, new IntOpenHashSet(), parent);
  }

  /**
   * Get the index set of the rows that need to fetch.
   *
   * @return IntOpenHashSet the index set of rows that need to fetch
   */
  public IntOpenHashSet getRowIds() {
    return rowIdSet;
  }

  /**
   * Add a row index to the index set of the rows that need to fetch.
   *
   * @param rowId int row index
   */
  public void addRowId(int rowId) {
    rowIdSet.add(rowId);
  }

  /**
   * Get the parent RowIndex instance.
   *
   * @return RowIndex the parent RowIndex instance
   */
  public RowIndex getParent() {
    return parent;
  }

  /**
   * Get matrix id.
   *
   * @return int matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Set matrix id
   *
   * @param matrixId matrix id
   */
  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  /**
   * Get the index set of the rows that have been found in matrix storage or matrix cache.
   *
   * @return IntOpenHashSet the index set of rows that have been found in matrix storage or matrix
   * cache
   */
  public IntOpenHashSet getFiltedIdSet() {
    return filtedIdSet;
  }

  /**
   * Add a row index to the index set of the rows that have been found in matrix storage or matrix
   * cache.
   *
   * @param rowId row index
   */
  public void filted(int rowId) {
    filtedIdSet.add(rowId);
  }

  /**
   * Clear the the index set of the rows that have been found in matrix storage or matrix cache.
   */
  public void clearFilted() {
    filtedIdSet.clear();
  }

  /**
   * Is the row has been found in matrix storage or matrix cache.
   *
   * @param rowId row index
   * @return boolean true means the row that has been found in matrix storage or matrix cache
   */
  public boolean isFilted(int rowId) {
    return filtedIdSet.contains(rowId);
  }

  @Override public String toString() {
    return "RowIndex [rowIds size=" + getRowsNumber() + ", indexHashCode=" + uuid + ", parent="
      + parent + ", matrixId=" + matrixId + "]";
  }

  public String listRowIds() {
    int[] idList = new int[rowIdSet.size()];
    IntIterator iter = rowIdSet.iterator();
    int i = 0;
    while (iter.hasNext()) {
      idList[i++] = iter.nextInt();
    }

    Arrays.sort(idList);

    StringBuilder sb = new StringBuilder();
    for (i = 0; i < idList.length; i++) {
      sb.append(idList[i]);
      sb.append(",");
    }

    return sb.toString();
  }

  /**
   * Get the number of the rows that need to fetch.
   *
   * @return int the number of the rows that need to fetch
   */
  public int getRowsNumber() {
    return rowIdSet.size();
  }

  /**
   * Get the number of the rows that need to fetch.
   *
   * @return int the number of the rows that have been found in matrix storage or matrix cache
   */
  public int getFiltedRowsNumber() {
    return filtedIdSet.size();
  }

  /**
   * If the row index is in the set of the rows that need to fetch.
   *
   * @param rowId row index
   * @return boolean true means the row index is in the set of the rows that need to fetch
   */
  public boolean contains(int rowId) {
    return rowIdSet.contains(rowId);
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RowIndex other = (RowIndex) obj;
    if (uuid == null) {
      if (other.uuid != null)
        return false;
    } else if (!uuid.equals(other.uuid))
      return false;
    return true;
  }
}
