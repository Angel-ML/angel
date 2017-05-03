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
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupState;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static com.tencent.angel.webapp.AngelParams.WORKERGROUP_STATE;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class WorkerGroupsBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  WorkerGroupsBlock(AMContext amctx) {
    amContext = amctx;
  }

  private Set<AMWorkerGroupState> transformToInternalState(String state) {
    LOG.info("get workergroup state is " + $(WORKERGROUP_STATE));
    Set<AMWorkerGroupState> stateSet = new HashSet<AMWorkerGroupState>();
    switch (state) {
      case "NEW":
        stateSet.add(AMWorkerGroupState.NEW);
        stateSet.add(AMWorkerGroupState.INITED);
        break;
      case "RUNNING":
        stateSet.add(AMWorkerGroupState.RUNNING);
        break;
      case "FAILED":
        stateSet.add(AMWorkerGroupState.FAILED);
        break;
      case "KILLED":
        stateSet.add(AMWorkerGroupState.KILLED);
        break;
      case "SUCCESS":
        stateSet.add(AMWorkerGroupState.SUCCESS);
        break;
    }
    return stateSet;
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel WorkerGroups ", $(WORKERGROUP_STATE)));
    TABLE<Hamlet> table = html.table("#job");
    TR<THEAD<TABLE<Hamlet>>> tr = table.thead().tr();

    tr.th(_TH, "id").th(_TH, "state").th(_TH, "leader").th(_TH, "start time").th(_TH, "end time")
        .th(_TH, "elapsed time");

    tr._()._();

    Set<AMWorkerGroupState> stateSet = transformToInternalState($(WORKERGROUP_STATE));

    TBODY<TABLE<Hamlet>> tbody = table.tbody();

    LOG.info("before get groups, group size is "
        + amContext.getWorkerManager().getWorkerGroupMap().size());
    for (AMWorkerGroupState s : stateSet) {
      LOG.info("s = " + s);
    }

    for (AMWorkerGroup workerGroup : amContext.getWorkerManager().getWorkerGroupMap().values()) {
      LOG.info("group state is " + workerGroup.getState());
      if (stateSet.contains(workerGroup.getState())) {
        TR<TBODY<TABLE<Hamlet>>> tr1 = tbody.tr();
        long elaspedTs = 0;
        if (workerGroup.getLaunchTime() != 0 && workerGroup.getFinishTime() != 0) {
          elaspedTs = workerGroup.getFinishTime() - workerGroup.getLaunchTime();
        } else if (workerGroup.getLaunchTime() != 0 && workerGroup.getFinishTime() == 0) {
          elaspedTs = System.currentTimeMillis() - workerGroup.getLaunchTime();
        }

        tr1.td()
            .a(url("angel/workerGroupPage/", workerGroup.getId().toString()),
                workerGroup.getId().toString())
            ._()
            .td($(WORKERGROUP_STATE))
            .td(workerGroup.getLeader().toString())
            .td(workerGroup.getLaunchTime() == 0 ? "N/A" : new Date(workerGroup.getLaunchTime())
                .toString())
            .td(workerGroup.getFinishTime() == 0 ? "N/A" : new Date(workerGroup.getFinishTime())
                .toString()).td(elaspedTs == 0 ? "N/A" : StringUtils.formatTime(elaspedTs));
        tr1._();
      }
    }
    tbody._()._();
  }
}
