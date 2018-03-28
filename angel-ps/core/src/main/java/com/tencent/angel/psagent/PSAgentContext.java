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

package com.tencent.angel.psagent;

import com.tencent.angel.RunningMode;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ml.matrix.transport.PSFailedReport;
import com.tencent.angel.protobuf.generated.MLProtos.PSAgentAttemptIdProto;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.client.PSControlClientManager;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.consistency.ConsistencyController;
import com.tencent.angel.psagent.executor.Executor;
import com.tencent.angel.psagent.matrix.MatrixClientFactory;
import com.tencent.angel.psagent.matrix.PSAgentLocationManager;
import com.tencent.angel.psagent.matrix.PSAgentMatrixMetaManager;
import com.tencent.angel.psagent.matrix.cache.MatricesCache;
import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLogCache;
import com.tencent.angel.psagent.matrix.storage.MatrixStorageManager;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.adapter.MatrixClientAdapter;
import com.tencent.angel.psagent.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ps agent context, it is used to share information between the all components in ps agent
 */
public class PSAgentContext {
  private static final Log LOG = LogFactory.getLog(PSAgentContext.class);
  private static PSAgentContext context = new PSAgentContext();
  
  /**ps agent*/
  private volatile PSAgent psAgent;
  
  /**task id to task context map*/
  private final ConcurrentHashMap<Integer, TaskContext> taskContexts;
  
  private PSAgentContext() {
    taskContexts = new ConcurrentHashMap<Integer, TaskContext>();
  }

  /**
   * Get the single instance of PSAgentContext
   * 
   * @return PSAgentContext the single instance of PSAgentContext
   */
  public static PSAgentContext get() {
    return context;
  }

  /**
   * Get ps agent
   * 
   * @return  PSAgent
   */
  public PSAgent getPsAgent() {
    return psAgent;
  }

  /**
   * Set ps agent
   * 
   * @param psAgent ps agent
   */
  public void setPsAgent(PSAgent psAgent) {
    this.psAgent = psAgent;
  }

  /**
   * Get application configuration
   * 
   * @return Configuration application configuration
   */
  public Configuration getConf() {
    return psAgent.getConf();
  }

  /**
   * Get ps agent metrics
   * 
   * @return Map<String, String>  ps agent metrics
   */
  public Map<String, String> getMetrics() {
    return psAgent.getMetrics();
  }

  /**
   * Get rpc client to master
   * 
   * @return MasterClient  rpc client to master
   */
  public MasterClient getMasterClient() {
    return psAgent.getMasterClient();
  }


  /**
   * Get matrix update cache
   * 
   * @return MatrixOpLogCache matrix update cache
   */
  public MatrixOpLogCache getOpLogCache() {
    return psAgent.getOpLogCache();
  }

  /**
   * Get rpc client to ps
   * 
   * @return MatrixTransportClient rpc client to ps
   */
  public MatrixTransportClient getMatrixTransportClient() {
    return psAgent.getMatrixClient();
  }

  /**
   * Get matrix meta manager
   * 
   * @return MatrixMetaManager matrix meta manager
   */
  public PSAgentMatrixMetaManager getMatrixMetaManager() {
    return psAgent.getMatrixMetaManager();
  }

  /**
   * Get the total task number in the application
   * 
   * @return int the total task number in the application
   */
  public int getTotalTaskNum() {
    return getConf().getInt(AngelConf.ANGEL_TASK_ACTUAL_NUM, 1);
  }

  /**
   * Get ps location cache
   * 
   * @return LocationCache ps location cache
   */
  public PSAgentLocationManager getLocationManager() {
    return psAgent.getLocationManager();
  }

  /**
   * Get rpc try interval in milliseconds
   * 
   * @return long rpc try interval in milliseconds
   */
  public long getRequestSleepTimeMS() {
    return getConf().getInt(AngelConf.ANGEL_REQUEST_SLEEP_TIME_MS,
        AngelConf.DEFAULT_ANGEL_REQUEST_SLEEP_TIME_MS);
  }

  /**
   * Get maximum network bytes being transmitted
   * 
   * @return long maximum network bytes being transmitted
   */
  public long getMaxBytesInFlight() {
    return getConf().getLong(AngelConf.ANGEL_NETWORK_MAX_BYTES_FLIGHT,
        AngelConf.DEFAULT_ANGEL_NETWORK_MAX_BYTES_FLIGHT);
  }
  
  /**
   * If report clock to master with sync mode
   * 
   * @return true mean use sync mode, false mean use async mode
   */
  public boolean syncClockEnable(){
    return getConf()
            .getBoolean(AngelConf.ANGEL_PSAGENT_SYNC_CLOCK_ENABLE,
                AngelConf.DEFAULT_ANGEL_PSAGENT_SYNC_CLOCK_ENABLE);
  }

  /**
   * Get application running mode
   *
   * @return RunningMode application running mode
   */
  public RunningMode getRunningMode() {
    return psAgent.getRunningMode();
  }

  /**
   * Get ps agent location ip
   * 
   * @return String ps agent location ip
   */
  public String getIp() {
    return psAgent.getIp();
  }

  /**
   * Get SSP staleness value
   * 
   * @return int SSP staleness value
   */
  public int getStaleness() {
    return getConf().getInt(AngelConf.ANGEL_STALENESS,
        AngelConf.DEFAULT_ANGEL_STALENESS);
  }

  /**
   * Get task context for a task
   * 
   * @param taskIndex task index
   * @return TaskContext task context
   */
  public TaskContext getTaskContext(int taskIndex) {
    TaskContext context = taskContexts.get(taskIndex);
    if (context == null) {
      taskContexts.putIfAbsent(taskIndex, new TaskContext(taskIndex));
      context = taskContexts.get(taskIndex);
    }
    return context;
  }

  /**
   * Get SSP consistency controller
   * 
   * @return ConsistencyController SSP consistency controller
   */
  public ConsistencyController getConsistencyController() {
    return psAgent.getConsistencyController();
  }

  /**
   * Get matrix update cache
   * 
   * @return MatrixOpLogCache matrix update cache
   */
  public MatrixOpLogCache getMatrixOpLogCache() {
    return psAgent.getOpLogCache();
  }

  /**
   * Get matrix clock cache
   * 
   * @return ClockCache matrix clock cache
   */
  public ClockCache getClockCache() {
    return psAgent.getClockCache();
  }

  /**
   * Get matrix cache
   * 
   * @return ClockCache matrix cache
   */
  public MatricesCache getMatricesCache() {
    return psAgent.getMatricesCache();
  }

  /**
   * Get matrix storage manager
   *
   * @return MatrixStorageManager matrix storage manager
   */
  public MatrixStorageManager getMatrixStorageManager() {
    return psAgent.getMatrixStorageManager();
  }

  /**
   * Get application layer request adapter
   * 
   * @return MatrixClientAdapter application layer request adapter
   */
  public MatrixClientAdapter getMatrixClientAdapter() {
    return psAgent.getMatrixClientAdapter();
  }

  /**
   * Get the machine learning executor reference
   * 
   * @return Executor the machine learning executor reference
   */
  public Executor getExecutor() {
    return psAgent.getExecutor();
  }

  /**
   * Get local task number
   * 
   * @return int local task number
   */
  public int getLocalTaskNum() {
    return getExecutor().getTaskNum();
  }

  /**
   * Clear context
   */
  public void clear() {
    MatrixClientFactory.clear();
    psAgent = null;
    taskContexts.clear();
  }

  /**
   * Get PSAgent id
   * @return PSAgent id
   */
  public int getPSAgentId() {
    return psAgent.getId();
  }

  /**
   * Get control connection manager
   * @return control connection manager
   */
  public TConnection getControlConnectManager() {
    return psAgent.getControlConnectManager();
  }

  /**
   * Get ps control rpc client manager
   * @return ps control rpc client manager
   */
  public PSControlClientManager getPSControlClientManager() {
    return psAgent.getPsControlClientManager();
  }

  /**
   * Get psagent location
   * @return psagent location
   */
  public Location getLocation() {
    return psAgent.getLocation();
  }
}
