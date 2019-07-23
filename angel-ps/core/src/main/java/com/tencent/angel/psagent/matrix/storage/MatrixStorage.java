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


package com.tencent.angel.psagent.matrix.storage;

/**
 * The storage for a single matrix.
 */

import com.tencent.angel.ml.servingmath2.vector.Vector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MatrixStorage {
  /**
   * row index to row map
   */
  private final ConcurrentHashMap<Integer, Vector> rowIndexToRowMap;
  private final ReentrantReadWriteLock lock;

  /**
   * Create a new MatrixStorage.
   */
  public MatrixStorage() {
    rowIndexToRowMap = new ConcurrentHashMap<>();
    lock = new ReentrantReadWriteLock();
  }

  /**
   * Get the row from the storage.
   *
   * @param rowIndex row index
   * @return TVector row
   */
  public Vector getRow(int rowIndex) {
    return rowIndexToRowMap.get(rowIndex);
  }

  /**
   * Add the row to the storage.
   *
   * @param rowIndex row index
   * @param row
   */
  public void addRow(int rowIndex, Vector row) {
    rowIndexToRowMap.put(rowIndex, row);
  }

  /**
   * Remove the row from the storage.
   *
   * @param rowIndex row index
   */
  public void removeRow(int rowIndex) {
    rowIndexToRowMap.remove(rowIndex);
  }

  public ReentrantReadWriteLock getLock() {
    return lock;
  }
}
