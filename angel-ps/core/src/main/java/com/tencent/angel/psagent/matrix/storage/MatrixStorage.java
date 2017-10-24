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

package com.tencent.angel.psagent.matrix.storage;

/**
 * The storage for a single matrix.
 */

import com.tencent.angel.ml.math.TVector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MatrixStorage {
  /** row index to row map*/
  private final ConcurrentHashMap<Integer, TVector> rowIndexToRowMap;
  private final ReentrantReadWriteLock lock;

  /**
   * Create a new MatrixStorage.
   */
  public MatrixStorage() {
    rowIndexToRowMap = new ConcurrentHashMap<Integer, TVector>();
    lock = new ReentrantReadWriteLock();
  }

  /**
   * Get the row from the storage.
   * 
   * @param rowIndex row index
   * @return TVector row
   */
  public TVector getRow(int rowIndex) {
    return rowIndexToRowMap.get(rowIndex);
  }

  /**
   * Add the row to the storage.
   * 
   * @param rowIndex row index
   * @param TVector row
   */
  public void addRow(int rowIndex, TVector row) {
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
