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
package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.*;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class ProgressBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  ProgressBlock(AMContext amctx) {
    amContext = amctx;
  }


  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel Progress"));
    Hamlet.TABLE<Hamlet.DIV<Hamlet>> table = html.div(_INFO_WRAP).table("#job");
    Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<Hamlet.DIV<Hamlet>>>> headTr = table.thead().tr();

    headTr.th(_TH, "taskid").th(_TH, "state").th(_TH, "current iteration")
        .th(_TH, "workerlog");
    headTr._()._();
    float current_iteration_progress = (float) 0.0;
    float current_clock_progress = (float) 0.0;
    Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> tbody = table.tbody();
    List<AMTask> amTaskList = new ArrayList();
    Map<AMTask, WorkerAttempt> map = new HashMap<>();
    Collection<AMWorkerGroup> amWorkerGroupSet =
        amContext.getWorkerManager().getWorkerGroupMap().values();
    for (AMWorkerGroup amWorkerGroup : amWorkerGroupSet) {
      Collection<AMWorker> amWorkerSet = amWorkerGroup.getWorkerSet();
      for (AMWorker amWorker : amWorkerSet) {
        Collection<WorkerAttempt> workerAttempts = amWorker.getAttempts().values();
        for (WorkerAttempt workerAttempt : workerAttempts) {
          Collection<AMTask> amTasks = workerAttempt.getTaskMap().values();
          for (AMTask amTask : amTasks) {
            map.put(amTask, workerAttempt);
          }
        }
      }
    }

    for (AMTask amTask : amContext.getTaskManager().getTasks()) {
      amTaskList.add(amTask);
    }
    Collections.sort(amTaskList, new Comparator<AMTask>() {
      @Override
      public int compare(AMTask task1, AMTask task2) {
        return task1.getTaskId().getIndex() - task2.getTaskId().getIndex();
      }
    });
    for (AMTask task : amTaskList) {
      WorkerAttempt workerAttempt = map.get(task);
      if (task.getProgress() >= 0 && task.getProgress() <= 1)
        current_iteration_progress = task.getProgress();
      current_clock_progress =
          ((float) task.getIteration()) / ((float) amContext.getTotalIterationNum());
      Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>>> tr = tbody.tr();
      tr.td(task.getTaskId().toString()).td(task.getState().toString())
          .td(String.valueOf(task.getIteration()) + "/" + amContext.getTotalIterationNum())
          .td()
          .a(url(MRWebAppUtil.getYARNWebappScheme(), workerAttempt.getNodeHttpAddr(), "node",
              "containerlogs", workerAttempt.getContainerIdStr(), amContext.getUser().toString()),
              workerAttempt.getId().toString())
          ._();
      tr._();
    }

    tbody._()._()._();
  }
}
