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

package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;

import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.*;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Date;
import java.util.Map;

import static com.tencent.angel.webapp.AngelParams.WORKERGROUP_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class WorkerGroupBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  WorkerGroupBlock(AMContext amctx) {
    amContext = amctx;
  }

  @Override
  protected void render(Block html) {
    String workerGroupIdSr = $(WORKERGROUP_ID);
    if (workerGroupIdSr.isEmpty()) {
      html.p()._("Sorry, can't do anything without a WorkerGroupId.")._();
      return;
    }

    WorkerGroupId workerGroupId;
    try {
      workerGroupId = new WorkerGroupId(workerGroupIdSr);
    } catch (UnvalidIdStrException e) {
      LOG.error("unvalid id string, ", e);
      return;
    }
    AMWorkerGroup workerGroup = amContext.getWorkerManager().getWorkerGroup(workerGroupId);
    if (workerGroup == null) {
      html.p()._("Sorry, can't find group " + workerGroupId)._();
      return;
    }

    set(TITLE, join("Angel WorkerGroup ", $(WORKERGROUP_ID)));
    html.h1(workerGroupIdSr);

    TABLE<DIV<Hamlet>> table = html.div(_INFO_WRAP).table("#job");
    TR<THEAD<TABLE<DIV<Hamlet>>>> headTr = table.thead().tr();

    headTr.th(_TH, "id").th(_TH, "state").th(_TH, "node address").th(_TH, "start time")
        .th(_TH, "end time").th(_TH, "elapsed time").th(_TH, "log").th(_TH, "threadstack")
        .th(_TH, "workercounter");

    headTr._()._();

    TBODY<TABLE<DIV<Hamlet>>> tbody = table.tbody();
    for (AMWorker worker : workerGroup.getWorkerSet()) {
      Map<WorkerAttemptId, WorkerAttempt> workerAttempts = worker.getAttempts();
      for (WorkerAttempt workerAttempt : workerAttempts.values()) {
        TR<TBODY<TABLE<DIV<Hamlet>>>> tr = tbody.tr();
        long elaspedTs = 0;
        if (workerAttempt.getLaunchTime() != 0 && workerAttempt.getFinishTime() != 0) {
          elaspedTs = workerAttempt.getFinishTime() - workerAttempt.getLaunchTime();
        } else if (workerAttempt.getLaunchTime() != 0 && workerAttempt.getFinishTime() == 0) {
          elaspedTs = System.currentTimeMillis() - workerAttempt.getLaunchTime();
        }

        if (workerAttempt.getNodeHttpAddr() == null) {
          tr.td()
              .a(url("angel/workerPage", workerAttempt.getId().toString()),
                  workerAttempt.getId().toString())
              ._()
              .td(workerAttempt.getState().toString())
              .td("N/A")
              .td((workerAttempt.getLaunchTime() == 0) ? "N/A" : new Date(workerAttempt
                  .getLaunchTime()).toString())
              .td((workerAttempt.getFinishTime() == 0) ? "N/A" : new Date(workerAttempt
                  .getFinishTime()).toString())
              .td((elaspedTs == 0) ? "N/A" : StringUtils.formatTime(elaspedTs)).td("N/A").td("N/A")
              .td("N/A");
        } else {

          tr.td()
              .a(url("angel/workerPage", workerAttempt.getId().toString()),
                  workerAttempt.getId().toString())
              ._()
              .td(workerAttempt.getState().toString())
              .td()
              .a(url(MRWebAppUtil.getYARNWebappScheme(), workerAttempt.getNodeHttpAddr()),
                  workerAttempt.getNodeHttpAddr())
              ._()
              .td((workerAttempt.getLaunchTime() == 0) ? "N/A" : new Date(workerAttempt
                  .getLaunchTime()).toString())
              .td((workerAttempt.getFinishTime() == 0) ? "N/A" : new Date(workerAttempt
                  .getFinishTime()).toString())
              .td((elaspedTs == 0) ? "N/A" : StringUtils.formatTime(elaspedTs))
              .td()
              .a(url(MRWebAppUtil.getYARNWebappScheme(), workerAttempt.getNodeHttpAddr(), "node",
                  "containerlogs", workerAttempt.getContainerIdStr(), amContext.getUser()
                      .toString()), "log")
              ._()
              .td()
              .a(url("angel/workerThreadStackPage/", workerAttempt.getId().toString()),
                  "workerthreadstack")
              ._()
              .td()
              .a(url("angel/workerCounterPage/", workerAttempt.getId().toString()), "workercounter")
              ._();
        }
        tr._();
      }
    }

    tbody._()._()._();
  }
}
