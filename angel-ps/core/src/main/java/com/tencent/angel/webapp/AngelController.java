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


package com.tencent.angel.webapp;

import com.google.inject.Inject;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.webapp.page.*;
import org.apache.hadoop.yarn.webapp.Controller;

public class AngelController extends Controller implements AngelParams {
  protected final AMContext appContext;

  // The app injection is optional
  @Inject AngelController(AMContext appCtx, RequestContext ctx) {
    super(ctx);
    this.appContext = appCtx;
  }

  @Override public void index() {
    render(AngelAppPage.class);
    // renderText("hello world");
  }

  public void workerGroupsPage() {
    render(WorkerGroupsPage.class);
  }

  public void parameterServersPage() {
    render(ParameterServersPage.class);
  }

  public void parameterServerThreadStackPage() {
    render(ParameterServerThreadStackPage.class);

  }

  public void workerThreadStackPage() {
    render(WorkerThreadStackPage.class);

  }

  // user choose a workerGroupID from the workergroups page,
  // now we should change the AngelApp params and render the workergroup page;
  public void workerGroupPage() {
    render(WorkerGroupPage.class);
  }

  public void workerPage() {
    render(WorkerPage.class);
  }

  public void environmentPage() {
    render(EnvironmentPage.class);
  }

  public void executorsPage() {
    render(ExecutorsPage.class);
  }

  public void progressPage() {
    render(ProgressPage.class);
  }

  public void workerCounterPage() {
    render(WorkerCounterPage.class);
  }

  public void taskCountersPage() {
    render(TaskCountersPage.class);
  }
}
