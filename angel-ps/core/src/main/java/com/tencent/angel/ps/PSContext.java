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

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.client.MasterClient;
import com.tencent.angel.ps.client.PSLocationManager;
import com.tencent.angel.ps.clock.ClockVectorManager;
import com.tencent.angel.ps.io.PSModelIOExecutor;
import com.tencent.angel.ps.io.save.SnapshotDumper;
import com.tencent.angel.ps.meta.PSMatrixMetaManager;
import com.tencent.angel.ps.server.control.ParameterServerService;
import com.tencent.angel.ps.server.data.PSFailedReport;
import com.tencent.angel.ps.server.data.RunningContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.storage.MatrixStorageManager;
import org.apache.hadoop.conf.Configuration;

/**
 * Context of parameter server.
 */
public class PSContext {
  /**
   * PS
   */
  private final ParameterServer ps;

  /**
   * Create a PSContext
   *
   * @param ps PS
   */
  public PSContext(ParameterServer ps) {
    this.ps = ps;
  }

  /**
   * Get application total task number
   *
   * @return application total task number
   */
  public int getTaskNum() {
    return getConf().getInt(AngelConf.ANGEL_TASK_ACTUAL_NUM, 1);
  }

  /**
   * Get application configuration
   *
   * @return application configuration
   */
  public Configuration getConf() {
    return ps.getConf();
  }

  /**
   * Get PS
   *
   * @return PS
   */
  public ParameterServer getPs() {
    return ps;
  }

  /**
   * Get application deploy mode
   *
   * @return application deploy mode
   */
  public AngelDeployMode getDeployMode() {
    String mode =
      ps.getConf().get(AngelConf.ANGEL_DEPLOY_MODE, AngelConf.DEFAULT_ANGEL_DEPLOY_MODE);

    if (mode.equals(AngelDeployMode.LOCAL.toString())) {
      return AngelDeployMode.LOCAL;
    } else if (mode.equals(AngelDeployMode.KUBERNETES.toString())) {
      return AngelDeployMode.KUBERNETES;
    } else {
      return AngelDeployMode.YARN;
    }
  }

  /**
   * Get the RPC client to Master
   *
   * @return the RPC client to Master
   */
  public MasterClient getMaster() {
    return ps.getMaster();
  }

  /**
   * Get Matrix meta manager
   *
   * @return Matrix meta manager
   */
  public PSMatrixMetaManager getMatrixMetaManager() {
    return ps.getMatrixMetaManager();
  }

  /**
   * Get clock vector manager
   *
   * @return clock vector manager
   */
  public ClockVectorManager getClockVectorManager() {
    return ps.getClockVectorManager();
  }

  /**
   * Get matrix storage manager
   *
   * @return matrix storage manager
   */
  public MatrixStorageManager getMatrixStorageManager() {
    return ps.getMatrixStorageManager();
  }

  /**
   * Get location manager
   *
   * @return location manager
   */
  public PSLocationManager getLocationManager() {
    return ps.getLocationManager();
  }

  /**
   * Get ps attempt id
   *
   * @return ps attempt id
   */
  public PSAttemptId getPSAttemptId() {
    return ps.getPSAttemptId();
  }

  /**
   * Get the RPC server for control message
   *
   * @return
   */
  public ParameterServerService getPsService() {
    return ps.getPsService();
  }

  /**
   * Get RPC worker pool for matrix transformation
   *
   * @return RPC worker pool for matrix transformation
   */
  public WorkerPool getWorkerPool() {
    return ps.getWorkerPool();
  }

  /**
   * Get matrices load/save worker pool
   *
   * @return matrices load/save worker pool
   */
  public PSModelIOExecutor getIOExecutors() {
    return ps.getPSModelIOExecutor();
  }

  /**
   * Get snapshot dumper
   *
   * @return snapshot dumper
   */
  public SnapshotDumper getSnapshotDumper() {
    return ps.getSnapshotDumper();
  }

  /**
   * Get the replication number for a matrix partition
   *
   * @return the replication number for a matrix partition
   */
  public int getPartReplication() {
    return getConf().getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
      AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);
  }

  /**
   * Get the application running mode
   *
   * @return the application running mode
   */
  public RunningMode getRunningMode() {
    String modeStr =
      getConf().get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE);
    return RunningMode.valueOf(modeStr);
  }

  public PSFailedReport getPSFailedReport() {
    return ps.getPSFailedReport();
  }

  public RunningContext getRunningContext() {
    return ps.getRunningContext();
  }
}
