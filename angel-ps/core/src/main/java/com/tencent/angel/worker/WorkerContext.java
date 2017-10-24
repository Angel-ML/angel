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

package com.tencent.angel.worker;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerIdProto;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.worker.storage.DataBlockManager;
import com.tencent.angel.worker.task.TaskManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Map;

/**
 * The context for Worker
 */
public class WorkerContext {
  private Worker worker;
  private final static WorkerContext context = new WorkerContext();

  private WorkerContext() {
    super();
  }

  /**
   * Get worker context.
   *
   * @return the worker context
   */
  public static WorkerContext get() {
    return context;
  }

  /**
   * Sets worker on context
   *
   * @param worker the worker
   */
  public void setWorker(Worker worker) {
    this.worker = worker;
  }

  /**
   * Gets worker.
   *
   * @return the worker
   */
  public Worker getWorker() {
    return worker;
  }

  /**
   * Gets conf.
   *
   * @return the conf
   */
  public Configuration getConf() {
    return worker.getConf();
  }

  /**
   * Gets task manager.
   *
   * @return the task manager
   */
  public TaskManager getTaskManager() {
    return worker.getTaskManager();
  }

  /**
   * Gets data block manager.
   *
   * @return the data block manager
   */
  public DataBlockManager getDataBlockManager() {
    return worker.getDataBlockManager();
  }

  /**
   * Gets worker id.
   *
   * @return the worker id
   */
  public WorkerId getWorkerId() {
    return worker.getWorkerId();
  }

  /**
   * Gets application id.
   *
   * @return the application id
   */
  public ApplicationId getAppId() {
    return worker.getAppId();
  }

  /**
   * Gets init min clock.
   *
   * @return the init min clock
   */
  public int getInitMinClock() {
    return worker.getInitMinClock();
  }

  /**
   * Gets the user who submit application
   *
   * @return the user
   */
  public String getUser() {
    return worker.getUser();
  }

  /**
   * Gets worker metrics.
   *
   * @return the worker metrics
   */
  public Map<String, String> getWorkerMetrics() {
    return worker.getMetrics();
  }

  /**
   * Gets active task num.
   *
   * @return the active task num
   */
  public int getActiveTaskNum() {
    return worker.getActiveTaskNum();
  }

  /**
   * Gets ps agent.
   *
   * @return the ps agent
   */
  public PSAgent getPSAgent() {
    return worker.getPSAgent();
  }

  /**
   * Gets worker id proto.
   *
   * @return the worker id proto
   */
  public WorkerIdProto getWorkerIdProto() {
    return worker.getWorkerIdProto();
  }

  /**
   * Get worker location(ip and listening port)
   * 
   * @return Location worker location(ip and listening port)
   */
  public Location getLocation() {
    return worker.getWorkerService().getLocation();
  }

  /**
   * Gets worker group id.
   *
   * @return the worker group id
   */
  public WorkerGroupId getWorkerGroupId() {
    return worker.getWorkerGroupId();
  }

  /**
   * Gets request sleep time ms.
   *
   * @return the request sleep time ms
   */
  public long getRequestSleepTimeMS() {
    return 1000;
  }

  /**
   * Gets worker attempt id.
   *
   * @return the worker attempt id
   */
  public WorkerAttemptId getWorkerAttemptId() {
    return worker.getWorkerAttemptId();
  }

  /**
   * Gets worker attempt id proto.
   *
   * @return the worker attempt id proto
   */
  public WorkerAttemptIdProto getWorkerAttemptIdProto() {
    return worker.getWorkerAttemptIdProto();
  }

  /**
   * Gets deploy mode.
   *
   * @return the deploy mode(YARN/LOCAL)
   */
  public AngelDeployMode getDeployMode() {
    String mode =
        worker.getConf().get(AngelConf.ANGEL_DEPLOY_MODE,
            AngelConf.DEFAULT_ANGEL_DEPLOY_MODE);

    if (mode.equals(AngelDeployMode.LOCAL.toString())) {
      return AngelDeployMode.LOCAL;
    } else {
      return AngelDeployMode.YARN;
    }
  }
}
