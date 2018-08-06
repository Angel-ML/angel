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
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static com.tencent.angel.webapp.AngelParams.WORKER_ATTEMPT_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class WorkerBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  WorkerBlock(AMContext amctx) {
    amContext = amctx;
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel Worker Attempt ", $(WORKER_ATTEMPT_ID)));
    String workerAttemptIdStr = $(WORKER_ATTEMPT_ID);
    if (workerAttemptIdStr == null || workerAttemptIdStr.isEmpty()) {
      html.p()._("Sorry, can't do anything without a WorkerId.")._();
      return;
    }

    WorkerAttemptId workerAttemptId = null;
    try {
      workerAttemptId = new WorkerAttemptId(workerAttemptIdStr);
    } catch (UnvalidIdStrException e) {
      LOG.error("unvalid id string, ", e);
      return;
    }
    AMWorker worker;

    worker = amContext.getWorkerManager().getWorker(workerAttemptId.getWorkerId());
    if (worker == null) {
      html.p()._("Sorry, can't find worker " + workerAttemptId.getWorkerId())._();
      return;
    }

    WorkerAttempt workerAttempt = worker.getWorkerAttempt(workerAttemptId);

    TABLE<DIV<Hamlet>> table = html.div(_INFO_WRAP).table("#job");
    TR<THEAD<TABLE<DIV<Hamlet>>>> headTr = table.thead().tr();

    headTr.th(_TH, "taskid").th(_TH, "state").th(_TH, "current iteration")
        .th(_TH, "current iteration bar").th(_TH, "current progress")
        .th(_TH, "current progress bar").th(_TH, "taskcounters");
    headTr._()._();
    float current_iteration_progress = (float) 0.0;
    float current_clock_progress = (float) 0.0;
    TBODY<TABLE<DIV<Hamlet>>> tbody = table.tbody();
    for (AMTask task : workerAttempt.getTaskMap().values()) {
      if (task.getProgress() >= 0 && task.getProgress() <= 1)
        current_iteration_progress = task.getProgress();
      current_clock_progress =
          ((float) task.getIteration()) / ((float) amContext.getTotalIterationNum());
      TR<TBODY<TABLE<DIV<Hamlet>>>> tr = tbody.tr();
      tr.td(task.getTaskId().toString()).td(task.getState().toString())
          .td(String.valueOf(task.getIteration()) + "/" + amContext.getTotalIterationNum()).td()
          .div(_PROGRESSBAR).$title(join(String.valueOf(current_clock_progress * 100), '%')). // tooltip
          div(_PROGRESSBAR_VALUE)
          .$style(join("width:", String.valueOf(current_clock_progress * 100), '%'))._()._()._()
          .td(String.valueOf(current_iteration_progress)).td().div(_PROGRESSBAR)
          .$title(join(String.valueOf(current_iteration_progress * 100), '%'))
          .div(_PROGRESSBAR_VALUE)
          .$style(join("width:", String.valueOf(current_iteration_progress * 100), '%'))._()._()._()
          .td().a(url("angel/taskCountersPage/", task.getTaskId().toString()), "taskcounters")._();
      tr._();
    }

    tbody._()._()._();
  }
}
