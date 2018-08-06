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

import org.apache.hadoop.mapreduce.v2.app.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

public class AngelWebApp extends WebApp implements AngelParams {

  @Override
  public void setup() {
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    route("/", AngelController.class);
    route("/angel", AngelController.class);

    route(pajoin("/angel/workerGroupsPage", WORKERGROUP_STATE), AngelController.class,
        "workerGroupsPage");

    route(pajoin("/angel/parameterServersPage", PARAMETERSERVER_STATE), AngelController.class,
        "parameterServersPage");

    route(pajoin("/angel/workerGroupPage", WORKERGROUP_ID), AngelController.class,
        "workerGroupPage");

    route(pajoin("/angel/workerPage", WORKER_ATTEMPT_ID), AngelController.class, "workerPage");

    route(pajoin("/angel/parameterServerThreadStackPage", PSATTEMPT_ID), AngelController.class,
        "parameterServerThreadStackPage");

    route(pajoin("/angel/workerThreadStackPage", WORKER_ATTEMPT_ID), AngelController.class,
        "workerThreadStackPage");

    route(pajoin("/angel/workerCounterPage", WORKER_ATTEMPT_ID), AngelController.class,
        "workerCounterPage");

    route(pajoin("/angel/taskCountersPage", TASK_ID), AngelController.class, "taskCountersPage");

    route("/angel/EnvironmentPage", AngelController.class, "environmentPage");
    route("/angel/ExecutorsPage", AngelController.class, "executorsPage");
    route("/angel/ProgressPage", AngelController.class, "progressPage");

  }
}
