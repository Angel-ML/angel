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
 *
 */

package com.tencent.angel.master.slowcheck;

import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.task.TaskCounter;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * A simple slow workers check policy. It checks the calculate rate of all tasks. If a task calculate
 * rate is slower than the average rate * a setting discount, it means the worker that running this task
 * is a slow worker.
 */
public class TaskCalPerfChecker extends CheckPolicy {
  private static final Log LOG = LogFactory.getLog(TaskCalPerfChecker.class);
  /**
   * Create a TaskCalPerfChecker
   */
  public TaskCalPerfChecker(){
    super();
  }

  @Override
  public List<Id> check(AMContext context) {
    double slowestDiscount = context.getConf().getDouble(AngelConf.ANGEL_AM_TASK_SLOWEST_DISCOUNT,
      AngelConf.DEFAULT_ANGEL_AM_TASK_SLOWEST_DISCOUNT);

    LOG.info("start to check slow workers use TaskCalPerfChecker policy, slowestDiscount = " + slowestDiscount);
    Set<Id> slowWorkers = new HashSet<Id>();
    AMTaskManager taskManage = context.getTaskManager();
    WorkerManager workerManager = context.getWorkerManager();
    Collection<AMTask> tasks = taskManage.getTasks();

    long totalSamples = 0;
    long totalCalTimeMs = 0;
    double averageRate = 0.0;
    Map<TaskId, Double> taskIdToRateMap = new HashMap<TaskId, Double>(tasks.size());
    for(AMTask task:tasks) {
      if(task.getMetrics().containsKey(TaskCounter.TOTAL_CALCULATE_SAMPLES)
        && task.getMetrics().containsKey(TaskCounter.TOTAL_CALCULATE_TIME_MS)) {
        long sampleNum = Long.valueOf(task.getMetrics().get(TaskCounter.TOTAL_CALCULATE_SAMPLES));
        double calTimeMs = Long.valueOf(task.getMetrics().get(TaskCounter.TOTAL_CALCULATE_TIME_MS));
        LOG.info("for task " + task.getTaskId() + ", sampleNum = " + sampleNum + ", calTimeMs = " + calTimeMs);
        totalSamples += sampleNum;
        totalCalTimeMs += calTimeMs;
        if(sampleNum > 5000000) {
          LOG.info("task " + task.getTaskId() + " calculate rate = " + (calTimeMs * 10000 / sampleNum));
          taskIdToRateMap.put(task.getTaskId(), calTimeMs * 10000 / sampleNum);
        }
      }
    }

    if(totalSamples != 0) {
      averageRate = (double)totalCalTimeMs * 10000 / totalSamples;
    }

    LOG.info("totalSamples = " + totalSamples + ", totalCalTimeMs = "
      + totalCalTimeMs + ", average calulate time for 10000 samples = " + averageRate
      + ", the maximum calulate time for 10000 sample = " + averageRate / slowestDiscount);

    for(Map.Entry<TaskId, Double> rateEntry:taskIdToRateMap.entrySet()) {
      if(averageRate < rateEntry.getValue() * slowestDiscount) {
        LOG.info("task " + rateEntry.getKey() + " rate = " + rateEntry.getValue() + " is < " + averageRate * slowestDiscount);
        AMWorker worker = workerManager.getWorker(rateEntry.getKey());
        if(worker != null) {
          LOG.info("put worker " + worker.getId() + " to slow worker list");
          slowWorkers.add(worker.getId());
        }
      }
    }

    List<Id> slowWorkerList = new ArrayList<>(slowWorkers.size());
    slowWorkerList.addAll(slowWorkers);
    return slowWorkerList;
  }
}
