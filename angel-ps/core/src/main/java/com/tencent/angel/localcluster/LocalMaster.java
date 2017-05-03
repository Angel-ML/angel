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

package com.tencent.angel.localcluster;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.master.AngelApplicationMaster;

public class LocalMaster extends Thread {
  private static final Log LOG = LogFactory.getLog(LocalMaster.class);
  private final AngelApplicationMaster appMaster;
  private final ApplicationAttemptId appAttemptId;
  public LocalMaster(ApplicationAttemptId appAttemptId) throws IllegalArgumentException, IOException {
    this.appAttemptId = appAttemptId;
    LocalClusterContext clusterContext = LocalClusterContext.get();
    Configuration conf = clusterContext.getConf();
    conf.setBoolean("fs.automatic.close", false);
    String appName = conf.get(AngelConfiguration.ANGEL_JOB_NAME, "local-test");

    appMaster =
        new AngelApplicationMaster(conf, appName, appAttemptId,
            clusterContext.getContainerId(), clusterContext.getLocalHost(),
            clusterContext.getPort(), clusterContext.getHttpPort(), System.currentTimeMillis());
  }
  
  @Override
  public void run() {
    try {
      appMaster.initAndStart();
    } catch (Exception e) {
      LOG.fatal("master start failed.", e);
      LocalClusterContext.get().getLocalCluster().stop();
    }
  }

  public AngelApplicationMaster getAppMaster() {
    return appMaster;
  }

  public void exit() {
    appMaster.getAppContext().getApp().shouldRetry(false);
    appMaster.shutDownJob();
    interrupt();
  }
}
