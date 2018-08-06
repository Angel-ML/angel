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

package com.tencent.angel.ps.impl.matrix;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;

/**
 * This class track the update info of one row on PS.
 */
public class RowUpdateInfo {

  // Mapping from taskId to clock value.
  private Int2IntOpenHashMap clocks;

  public RowUpdateInfo() {
    clocks = new Int2IntOpenHashMap();
  }

  public int getTaskClock(int taskId) {
    return clocks.get(taskId);
  }

  public void setTaskClock(int taskId, int clock) {
    clocks.put(taskId, clock);
  }

  /**
   * Get min clock of this row, remove clocks of tasks which would never update this row.
   * 
   * @return min clock value
   */
  public int getMinClock() {
    if (clocks.size() == 0) {
      return 0;
    }

    IntIterator iter = clocks.values().iterator();
    int min = Integer.MAX_VALUE;
    while (iter.hasNext()) {
      int clock = iter.nextInt();
      if (clock < min) {
        min = clock;
      }
    }
    return min;
  }

  /**
   * Get active tasks num which update this row
   * 
   * @return active tasks num
   */
  public int getActiveTask() {
    return clocks.size();
  }
}
