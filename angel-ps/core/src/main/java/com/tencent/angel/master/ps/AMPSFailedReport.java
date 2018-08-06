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

package com.tencent.angel.master.ps;

import com.tencent.angel.ml.matrix.transport.PSFailedReport;
import com.tencent.angel.ml.matrix.transport.PSLocation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Master PS Failed counters
 */
public class AMPSFailedReport {
  private final int N = 10;
  private final float factor = 0.8f;
  private final PSFailedReport[] reports;
  private volatile int currentIndex = 0;

  public AMPSFailedReport() {
    reports = new PSFailedReport[N];
    for(int i = 0; i < N; i++) {
      reports[i] = new PSFailedReport();
    }
  }

  public void psFailedReports(Map<PSLocation, Integer> counters) {
    for(Map.Entry<PSLocation, Integer> entry : counters.entrySet()) {
      reports[currentIndex].psFailed(entry.getKey(), entry.getValue());
    }
  }

  private void clock() {
    currentIndex = (currentIndex + 1) % N;
  }

  public Map<PSLocation, Integer> getFailedPS(int limit) {
    Map<PSLocation, Integer> mergeCounters = new HashMap<>();
    float currentFactor = 1.0f;
    for(int i = 0; i < N; i++) {
      merge(reports[(currentIndex - i + N) % N].getReports(), mergeCounters, currentFactor);
      currentFactor *= factor;
    }

    if(mergeCounters.isEmpty()) {
      clock();
      return mergeCounters;
    } else {
      Map<PSLocation, Integer> result = new HashMap<>();
      for(Map.Entry<PSLocation, Integer> entry : mergeCounters.entrySet()) {
        if(entry.getValue() >= limit) {
          result.put(entry.getKey(), entry.getValue());
        }
      }
      clock();
      return result;
    }
  }

  private void merge(Map<PSLocation, Integer> source, Map<PSLocation, Integer> dest, float currentFactor) {
    for(Map.Entry<PSLocation, Integer> entry : source.entrySet()) {
      if(dest.containsKey(entry.getKey())) {
        dest.put(entry.getKey(), dest.get(entry.getKey()) + (int)(entry.getValue() * currentFactor));
      } else {
        dest.put(entry.getKey(), (int)(entry.getValue() * currentFactor));
      }
    }
  }
}
