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

package com.tencent.angel.worker;

import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Entry point of a {@link com.tencent.angel.AngelDeployMode#STANDALONE} Worker process.
 *
 * <p>It does NOT depend on YARN container env vars. The process expects the following env vars set
 * by {@link com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher}:
 * <ul>
 *   <li>{@code WORKER_GROUP_ID}</li>
 *   <li>{@code WORKER_ID}</li>
 *   <li>{@code WORKER_ATTEMPT_ID}</li>
 *   <li>{@code LISTEN_ADDR}</li>
 *   <li>{@code LISTEN_PORT}</li>
 *   <li>{@code WORKERGROUP_NUMBER}</li>
 *   <li>{@code TASK_NUMBER}</li>
 *   <li>{@code INIT_MIN_CLOCK}</li>
 *   <li>{@code ANGEL_USER_TASK} (optional, can be set in job.xml as well)</li>
 * </ul>
 *
 * <p>The application configuration is loaded from {@code job.xml} on the classpath. The
 * {@code ApplicationId} is read from {@link AngelConf#ANGEL_EMBEDDED_APP_ID}.
 */
public class StandaloneWorkerMain {

  private static final Log LOG = LogFactory.getLog(StandaloneWorkerMain.class);

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.addResource(AngelConf.ANGEL_JOB_CONF_FILE);
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");

    // Routable IP of this host as known by the launcher (ssh target). See the
    // analogous comment in StandalonePSMain.
    String workerListenHost = System.getenv(AngelEnvironment.WORKER_LISTEN_ADDR.name());
    if (workerListenHost != null && !workerListenHost.isEmpty()) {
      conf.set(AngelConf.ANGEL_WORKER_LISTEN_HOST, workerListenHost);
      LOG.info("Worker listen host (from launcher env) = " + workerListenHost);
    }

    String appIdStr = conf.get(AngelConf.ANGEL_EMBEDDED_APP_ID);
    if (appIdStr == null) {
      throw new IllegalStateException(
          AngelConf.ANGEL_EMBEDDED_APP_ID + " is not set in job.xml, cannot start standalone worker");
    }
    ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);

    String user = System.getenv("USER");
    if (user == null) {
      user = conf.get(AngelConf.USER_NAME, System.getProperty("user.name", "angel"));
    }

    int workerGroupIndex =
        Integer.parseInt(System.getenv(AngelEnvironment.WORKER_GROUP_ID.name()));
    int workerIndex = Integer.parseInt(System.getenv(AngelEnvironment.WORKER_ID.name()));
    int attemptIndex =
        Integer.parseInt(System.getenv(AngelEnvironment.WORKER_ATTEMPT_ID.name()));

    WorkerGroupId workerGroupId = new WorkerGroupId(workerGroupIndex);
    WorkerId workerId = new WorkerId(workerGroupId, workerIndex);
    WorkerAttemptId workerAttemptId = new WorkerAttemptId(workerId, attemptIndex);

    String workerGroupNum = System.getenv(AngelEnvironment.WORKERGROUP_NUMBER.name());
    if (workerGroupNum != null) {
      conf.set(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM, workerGroupNum);
    }
    String taskNum = System.getenv(AngelEnvironment.TASK_NUMBER.name());
    if (taskNum != null) {
      conf.set(AngelConf.ANGEL_TASK_ACTUAL_NUM, taskNum);
    }
    String userTask = System.getenv(AngelEnvironment.ANGEL_USER_TASK.name());
    if (userTask != null) {
      conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, userTask);
    }

    String masterAddr = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    int masterPort = Integer.parseInt(System.getenv(AngelEnvironment.LISTEN_PORT.name()));
    Location masterLocation = new Location(masterAddr, masterPort);

    String startClockEnv = System.getenv(AngelEnvironment.INIT_MIN_CLOCK.name());
    int startClock = startClockEnv == null ? 0 : Integer.parseInt(startClockEnv);

    LOG.info("Starting standalone Worker " + workerAttemptId
        + ", master=" + masterLocation + ", appId=" + appId);

    Worker worker = new Worker(AngelConf.clone(conf), appId, user, workerAttemptId, masterLocation,
        startClock, false);
    try {
      worker.initAndStart();
    } catch (Exception e) {
      LOG.fatal("Failed to start standalone worker.", e);
      worker.error(e.getMessage());
    }
  }
}
