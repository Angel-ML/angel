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
 */

package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.App;
import com.tencent.angel.master.ps.attempt.PSAttempt;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import java.util.Date;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class AngelAppBlock extends HtmlBlock {

  final AMContext amContext;

  @Inject
  AngelAppBlock(AMContext amctx) {
    amContext = amctx;
  }

  @Override
  protected void render(Block html) {

    set(TITLE, join("Angel Application", amContext.getApplicationId()));

    App app = amContext.getApp();

    long elaspedTs = 0;
    if (app.getLaunchTime() != 0 && app.getFinishTime() != 0) {
      elaspedTs = app.getFinishTime() - app.getLaunchTime();
    } else if (app.getLaunchTime() != 0 && app.getFinishTime() == 0) {
      elaspedTs = System.currentTimeMillis() - app.getLaunchTime();
    }

    info("Job Overview")._("Job Name:", amContext.getApplicationName())
        ._("State:", app.getExternAppState().toString())
        ._("Started:", new Date(app.getLaunchTime()))
        ._("Elapsed:", StringUtils.formatTime(elaspedTs))
        ._("Environment:", "nomeaning" == null ? "#" : "angel/EnvironmentPage",
            "Runtime Information And Properties")
        ._("Task Progress:", "nomeaning" == null ? "#" : "angel/ProgressPage", "progress")
        ._("Master Threaddump:", "nomeaning" == null ? "#" : "angel/ExecutorsPage", "threaddump");



    DIV<Hamlet> div = html._(InfoBlock.class).div(_INFO_WRAP);

    TABLE<DIV<Hamlet>> table = div.table("#job");
    table.tr().th(_TH, "module").th(_TH, "new").th(_TH, "running").th(_TH, "failed")
        .th(_TH, "killed").th(_TH, "success")._();

    int newGroupNum = 0;
    int runningGroupNum = 0;
    int failedGroupNum = 0;
    int killedGroupNum = 0;
    int successGroupNum = 0;

    int newPSNum = 0;
    int runningPSNum = 0;
    int failedPSNum = 0;
    int killedPSNum = 0;
    int successPSNum = 0;

    LOG.info("before compute worker state items");
    if (amContext.getWorkerManager() != null) {
      for (AMWorkerGroup group : amContext.getWorkerManager().getWorkerGroupMap().values()) {
        switch (group.getState()) {
          case NEW:
          case INITED:
            newGroupNum += 1;
            break;
          case RUNNING:
            runningGroupNum += 1;
            break;
          case KILLED:
            killedGroupNum += 1;
            break;
          case FAILED:
            failedGroupNum += 1;
            break;
          case SUCCESS:
            successGroupNum += 1;
            break;
          default:
            break;
        }
      }
    }

    for (AMParameterServer ps : amContext.getParameterServerManager().getParameterServerMap()
        .values()) {
      for (PSAttempt psAttemp : ps.getPSAttempts().values()) {
        switch (psAttemp.getInternalState()) {
          case NEW:
          case SCHEDULED:
          case LAUNCHED:
            newPSNum += 1;
            break;
          case RUNNING:
          case COMMITTING:
            runningPSNum += 1;
            break;
          case KILLED:
            killedPSNum += 1;
            break;
          case FAILED:
            failedPSNum += 1;
            break;
          case SUCCESS:
            successPSNum += 1;
            break;
          default:
            break;
        }
      }
    }

    table.tr().td("workergroups").td()
        .a(url("angel/workerGroupsPage", "NEW"), String.valueOf(newGroupNum))._().td()
        .a(url("angel/workerGroupsPage", "RUNNING"), String.valueOf(runningGroupNum))._().td()
        .a(url("angel/workerGroupsPage", "FAILED"), String.valueOf(failedGroupNum))._().td()
        .a(url("angel/workerGroupsPage", "KILLED"), String.valueOf(killedGroupNum))._().td()
        .a(url("angel/workerGroupsPage", "SUCCESS"), String.valueOf(successGroupNum))._()._().tr()
        .td("parameterservers").td()
        .a(url("angel/parameterServersPage", "NEW"), String.valueOf(newPSNum))._().td()
        .a(url("angel/parameterServersPage", "RUNNING"), String.valueOf(runningPSNum))._().td()
        .a(url("angel/parameterServersPage", "FAILED"), String.valueOf(failedPSNum))._().td()
        .a(url("angel/parameterServersPage", "KILLED"), String.valueOf(killedPSNum))._().td()
        .a(url("angel/parameterServersPage", "SUCCESS"), String.valueOf(successPSNum))._()._();

    table._();
    div._();
  }

}
