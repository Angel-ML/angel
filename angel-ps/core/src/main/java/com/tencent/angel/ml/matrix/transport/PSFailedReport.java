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

package com.tencent.angel.ml.matrix.transport;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PS failed counters
 */
public class PSFailedReport {
  private HashMap<PSLocation, Integer> psToFailedCounterMap;
  private final Lock lock;

  public PSFailedReport() {
    lock = new ReentrantLock();
    psToFailedCounterMap = new HashMap<>();
  }

  public void psFailed(PSLocation psLoc) {
    psFailed(psLoc, 1);
  }

  public void psFailed(PSLocation psLoc, int counter) {
    try {
      lock.lock();
      if(psToFailedCounterMap.containsKey(psLoc)) {
        psToFailedCounterMap.put(psLoc, psToFailedCounterMap.get(psLoc) + counter);
      } else {
        psToFailedCounterMap.put(psLoc, counter);
      }
    } finally {
      lock.unlock();
    }
  }

  public HashMap<PSLocation, Integer> getReports() {
    try {
      lock.lock();
      HashMap<PSLocation, Integer> ret = psToFailedCounterMap;
      psToFailedCounterMap = new HashMap<>();
      return ret;
    } finally {
      lock.unlock();
    }
  }
}
