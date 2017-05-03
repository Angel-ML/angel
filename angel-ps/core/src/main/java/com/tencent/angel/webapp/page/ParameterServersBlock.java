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
import com.tencent.angel.master.ps.attempt.PSAttempt;
import com.tencent.angel.master.ps.attempt.PSAttemptStateInternal;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.ps.PSAttemptId;

import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.tencent.angel.webapp.AngelParams.PARAMETERSERVER_STATE;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class ParameterServersBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  ParameterServersBlock(AMContext amctx) {
    amContext = amctx;
  }

  private Set<PSAttemptStateInternal> transformToInternalState(String state) {
    Set<PSAttemptStateInternal> stateSet = new HashSet<PSAttemptStateInternal>();
    switch (state) {
      case "NEW":
        stateSet.add(PSAttemptStateInternal.NEW);
        stateSet.add(PSAttemptStateInternal.INITED);
        stateSet.add(PSAttemptStateInternal.ASSIGNED);
        stateSet.add(PSAttemptStateInternal.SCHEDULED);
        stateSet.add(PSAttemptStateInternal.LAUNCHED);
        break;
      case "RUNNING":
        stateSet.add(PSAttemptStateInternal.RUNNING);
        stateSet.add(PSAttemptStateInternal.COMMITTING);
        break;
      case "FAILED":
        stateSet.add(PSAttemptStateInternal.FAILED);
        break;
      case "KILLED":
        stateSet.add(PSAttemptStateInternal.KILLED);
        break;
      case "SUCCESS":
        stateSet.add(PSAttemptStateInternal.SUCCESS);
        break;
    }
    return stateSet;
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel ParameterServers"));

    TABLE<Hamlet> table = html.table("#job");
    TR<THEAD<TABLE<Hamlet>>> headTr = table.thead().tr();

    headTr.th(_TH, "id").th(_TH, "state").th(_TH, "node address").th(_TH, "start time")
        .th(_TH, "end time").th(_TH, "elapsed time").th(_TH, "log").th(_TH, "threadstack");
    headTr._()._();

    Set<PSAttemptStateInternal> stateSet = transformToInternalState($(PARAMETERSERVER_STATE));

    TBODY<TABLE<Hamlet>> tbody = table.tbody();

    for (AMParameterServer ps : amContext.getParameterServerManager().getParameterServerMap()
        .values()) {

      Map<PSAttemptId, PSAttempt> psAttempts = ps.getPSAttempts();
      for (PSAttempt psAttempt : psAttempts.values()) {
        if (stateSet.contains(psAttempt.getInternalState())) {
          TR<TBODY<TABLE<Hamlet>>> tr = tbody.tr();
          long elaspedTs = 0;
          if (psAttempt.getLaunchTime() != 0 && psAttempt.getFinishTime() != 0) {
            elaspedTs = psAttempt.getFinishTime() - psAttempt.getLaunchTime();
          } else if (psAttempt.getLaunchTime() != 0 && psAttempt.getFinishTime() == 0) {
            elaspedTs = System.currentTimeMillis() - psAttempt.getLaunchTime();
          }

          if (psAttempt.getNodeHttpAddr() == null) {
            tr.td(psAttempt.getId().toString())
                .td($(PARAMETERSERVER_STATE))
                .td("N/A")
                .td(psAttempt.getLaunchTime() == 0 ? "N/A" : new Date(psAttempt.getLaunchTime())
                    .toString())
                .td(psAttempt.getFinishTime() == 0 ? "N/A" : new Date(psAttempt.getFinishTime())
                    .toString()).td(elaspedTs == 0 ? "N/A" : new Date(elaspedTs).toString())
                .td("N/A").td("N/A");
            tr._();
          } else {
            tr.td(psAttempt.getId().toString())
                .td($(PARAMETERSERVER_STATE))
                .td()
                .a(url(MRWebAppUtil.getYARNWebappScheme(), psAttempt.getNodeHttpAddr()),
                    psAttempt.getNodeHttpAddr())
                ._()
                .td(psAttempt.getLaunchTime() == 0 ? "N/A" : new Date(psAttempt.getLaunchTime())
                    .toString())
                .td(psAttempt.getFinishTime() == 0 ? "N/A" : new Date(psAttempt.getFinishTime())
                    .toString())
                .td(elaspedTs == 0 ? "N/A" : StringUtils.formatTime(elaspedTs))
                .td()
                .a(url(MRWebAppUtil.getYARNWebappScheme(), psAttempt.getNodeHttpAddr(), "node",
                    "containerlogs", psAttempt.getContainerIdStr(), amContext.getUser().toString()),
                    "log")
                ._()
                .td()
                .a(url("/angel/parameterServerThreadStackPage/", psAttempt.getId().toString()),
                    "psthreadstack")._();


            tr._();
          }
        }

      }
    }
    tbody._()._();
  }
}
