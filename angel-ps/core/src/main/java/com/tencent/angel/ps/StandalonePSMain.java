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

package com.tencent.angel.ps;

import com.tencent.angel.RunningMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.conf.AngelConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Entry point of a {@link com.tencent.angel.AngelDeployMode#STANDALONE} ParameterServer process.
 *
 * <p>It does NOT depend on YARN. The process expects the following environment variables to be set
 * by {@link com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher}:
 * <ul>
 *   <li>{@code PARAMETERSERVER_ID}</li>
 *   <li>{@code PS_ATTEMPT_ID}</li>
 *   <li>{@code LISTEN_ADDR}</li>
 *   <li>{@code LISTEN_PORT}</li>
 *   <li>{@code TASK_NUMBER}</li>
 * </ul>
 *
 * <p>The application configuration is loaded from {@code job.xml} which must be on the classpath.
 */
public class StandalonePSMain {

  private static final Log LOG = LogFactory.getLog(StandalonePSMain.class);

  public static void main(String[] argv) {
    LOG.info("Starting standalone Parameter Server");

    int serverIndex = Integer.parseInt(System.getenv(AngelEnvironment.PARAMETERSERVER_ID.name()));
    int attemptIndex = Integer.parseInt(System.getenv(AngelEnvironment.PS_ATTEMPT_ID.name()));
    String appMasterHost = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    int appMasterPort = Integer.parseInt(System.getenv(AngelEnvironment.LISTEN_PORT.name()));

    Configuration conf = new Configuration();
    conf.addResource(AngelConf.ANGEL_JOB_CONF_FILE);
    // Ensure deploy mode is STANDALONE in case the job.xml does not set it.
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");

    // Routable IP of this host as known by the launcher (ssh target). Propagated from
    // the master so the PS advertises a reachable address to psRegister, bypassing
    // InetAddress.getLocalHost() which returns 127.0.0.1 on boxes whose /etc/hosts
    // maps the hostname to loopback. A non-empty value always wins over the default
    // resolution path in com.tencent.angel.ps.client.MasterClient#register().
    String psListenHost = System.getenv(AngelEnvironment.PS_LISTEN_ADDR.name());
    if (psListenHost != null && !psListenHost.isEmpty()) {
      conf.set(AngelConf.ANGEL_PS_LISTEN_HOST, psListenHost);
      LOG.info("PS listen host (from launcher env) = " + psListenHost);
    }

    conf.setInt("io.file.buffer.size", conf.getInt(AngelConf.ANGEL_PS_IO_FILE_BUFFER_SIZE,
        AngelConf.DEFAULT_ANGEL_PS_IO_FILE_BUFFER_SIZE));

    String runningMode = conf.get(AngelConf.ANGEL_RUNNING_MODE,
        AngelConf.DEFAULT_ANGEL_RUNNING_MODE);
    String taskNumberEnv = System.getenv(AngelEnvironment.TASK_NUMBER.name());
    if (runningMode.equals(RunningMode.ANGEL_PS_WORKER.toString()) && taskNumberEnv != null) {
      conf.set(AngelConf.ANGEL_TASK_ACTUAL_NUM, taskNumberEnv);
    }

    final ParameterServer psServer =
        new ParameterServer(serverIndex, attemptIndex, appMasterHost, appMasterPort, conf);

    try {
      psServer.initialize();
      psServer.start();
      LOG.info("Standalone Parameter Server started successfully.");
    } catch (Throwable x) {
      LOG.fatal("Start standalone PS failed.", x);
      psServer.failed(x.getMessage());
    }
  }
}
