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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

public class LocalCluster {
  private static final Log LOG = LogFactory.getLog(LocalCluster.class);
  private final LocalResourceManager localRM;
  private final ApplicationId appId;
  public LocalCluster(Configuration conf, ApplicationId appId) {
    localRM = new LocalResourceManager(conf);
    this.appId = appId;
    LocalClusterContext.get().setLocalCluster(this);
  }
  
  public void start() throws IOException{
    localRM.start();
    localRM.allocateMaster(appId);
  }
  
  public void stop(){
    LOG.info("stop the cluster");
    Map<WorkerAttemptId, LocalWorker> localWorkers = LocalClusterContext.get().getIdToWorkerMap();
    for(LocalWorker localWorker:localWorkers.values()) {
      localWorker.exit();
    }
    
    Map<PSAttemptId, LocalPS> localPSs = LocalClusterContext.get().getIdToPSMap();
    for(LocalPS localPS:localPSs.values()) {
      localPS.exit();
    }
    
    LocalMaster master = LocalClusterContext.get().getMaster();
    if(master != null) {
      master.exit();
    }
    
    localRM.stop();
  }

  public LocalResourceManager getLocalRM() {
    return localRM;
  } 
}