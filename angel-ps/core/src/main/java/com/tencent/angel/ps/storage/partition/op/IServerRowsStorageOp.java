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

package com.tencent.angel.ps.storage.partition.op;

import com.tencent.angel.ps.storage.vector.ServerRow;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * Basic operation for Row-based storage
 */
public interface IServerRowsStorageOp {

  /**
   * Get the server row from storage use row id
   * @param rowId row id
   * @return server row
   */
  ServerRow getRow(int rowId);

  /**
   * Get batch of server rows use row ids
   * @param rowIds row ids
   * @return batch of server rows
   */
  List<ServerRow> getRows(List<Integer> rowIds);

  /**
   * Put the server row to the storage
   * @param rowId row id
   * @param row server row
   */
  void putRow(int rowId, ServerRow row);

  /**
   * Put the server rows to the storage
   * @param rowIds row ids
   * @param rows server rows
   */
  void putRows(List<Integer> rowIds, List<ServerRow> rows);

  /**
   * Get the row number
   * @return the row number
   */
  int getRowNum();

  /**
   * Is the server row exist
   * @param rowId row id
   * @return True means exist
   */
  boolean hasRow(int rowId);

  /**
   * Get the <row id, server row> iterator
   * @return the <row id, server row> iterator
   */
  Iterator<Entry<Integer, ServerRow>> iterator();
}
