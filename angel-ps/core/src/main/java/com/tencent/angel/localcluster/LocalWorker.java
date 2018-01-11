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

import com.tencent.angel.common.location.Location;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Local Angel Worker. It startups the {@link Worker} using a thread.
 */
public class LocalWorker extends Thread {
  private static final Log LOG = LogFactory.getLog(LocalCluster.class);
  private final Worker worker;

  /**
   * Create a local worker
   * @param conf cluster configuration
   * @param appId application id
   * @param user submit user name
   * @param workerAttemptId worker attempt id
   * @param masterLocation the location of master
   * @param initMinClock the start clock value
   * @param isLeader the worker is the leader of the workergroup or not
   */
  public LocalWorker(Configuration conf, ApplicationId appId, String user,
      WorkerAttemptId workerAttemptId, Location masterLocation, int initMinClock, boolean isLeader) {
    worker = new Worker(conf, appId, user, workerAttemptId, masterLocation, 0, false);
  }

  @Override
  public void run() {
    try {
      worker.initAndStart();
    } catch (Exception e) {
      LOG.fatal("worker " + worker.getWorkerAttemptId() + " start failed.", e);
      worker.error(e.getMessage());
    }
  }

  /**
   * Get worker
   * @return worker
   */
  public Worker getWorker() {
    return worker;
  }
  
  /**
   * Exit
   */
  public void exit(){
    worker.stop();
    interrupt();
  }
}
