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

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.RM_WEB;

public class NavBlock extends HtmlBlock {
  // final App app;
  //
  // @Inject NavBlock(App app) { this.app = app; }

  @Override
  protected void render(Block html) {
    String rmweb = $(RM_WEB);
    // DIV<Hamlet> nav =
    // html.div("#nav").h3("Cluster").ul().li().a(url(rmweb, "cluster", "cluster"), "About")._()
    // .li().a(url(rmweb, "cluster", "apps"), "Applications")._().li()
    // .a(url(rmweb, "cluster", "scheduler"), "Scheduler")._()._().h3("Application").ul().li()
    // .a(url("app/info"), "About")._().li().a(url("app"), "WorkerGroups")._()._();
    // if (app.getJob() != null) {
    // String jobid = MRApps.toString(app.getJob().getID());
    // List<AMInfo> amInfos = app.getJob().getAMInfos();
    // AMInfo thisAmInfo = amInfos.get(amInfos.size()-1);
    // String nodeHttpAddress = thisAmInfo.getNodeManagerHost() + ":"
    // + thisAmInfo.getNodeManagerHttpPort();
    // nav.
    // h3("Job").
    // ul().
    // li().a(url("job", jobid), "Overview")._().
    // li().a(url("jobcounters", jobid), "Counters")._().
    // li().a(url("conf", jobid), "Configuration")._().
    // li().a(url("tasks", jobid, "m"), "Map tasks")._().
    // li().a(url("tasks", jobid, "r"), "Reduce tasks")._().
    // li().a(".logslink", url(MRWebAppUtil.getYARNWebappScheme(),
    // nodeHttpAddress, "node",
    // "containerlogs", thisAmInfo.getContainerId().toString(),
    // app.getJob().getUserName()),
    // "AM Logs")._()._();
    // if (app.getTask() != null) {
    // String taskid = MRApps.toString(app.getTask().getID());
    // nav.
    // h3("Task").
    // ul().
    // li().a(url("task", taskid), "Task Overview")._().
    // li().a(url("taskcounters", taskid), "Counters")._()._();
    // }
    // }
    // nav.h3("Tools").ul().li().a("/conf", "Configuration")._().li().a("/logs", "Local logs")._()
    // .li().a("/stacks", "Server stacks")._().li().a("/metrics", "Server metrics")._()._()._();
  }
}
