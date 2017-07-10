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
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

/**
 * Local Angel cluster. It use a {@link LocalResourceManager} to allocate the resources of Angel
 * modules. A local cluster only support one Angel application now.
 */
public class LocalCluster {
  private static final Log LOG = LogFactory.getLog(LocalCluster.class);
  /**local resource manager*/
  private final LocalResourceManager localRM;
  
  /**application id*/
  private final ApplicationId appId;
  
  /**cluster id*/
  private final UUID id;
  
  /**
   * Create a new LocalCluster
   * @param conf cluster application
   * @param appId
   */
  public LocalCluster(Configuration conf, ApplicationId appId) {
    localRM = new LocalResourceManager(conf);
    this.appId = appId;
    id = UUID.randomUUID();
    LocalClusterContext.get().setLocalCluster(this);
  }
  
  /**
   * Start the cluster
   * @throws IOException
   */
  public void start() throws IOException{
    LOG.info("start local cluster " + id);
    localRM.start();
    localRM.allocateMaster(appId);
  }
  
  /**
   * Stop the cluster
   */
  public void stop(){
    LOG.info("stop the cluster " + id);
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

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
    }
    LOG.info("=============================================stop cluster over=======================================");
  }

  /**
   * Get local resource manager
   * @return local resource manager
   */
  public LocalResourceManager getLocalRM() {
    return localRM;
  } 
}
