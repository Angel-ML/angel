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

package com.tencent.angel.master.app;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.master.MasterService;
import com.tencent.angel.master.client.ClientManager;
import com.tencent.angel.master.data.DataSpliter;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.master.metrics.MetricsService;
import com.tencent.angel.master.oplog.AppStateStorage;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.master.psagent.PSAgentManager;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.WorkerManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.webapp.WebApp;

/**
 * Master context interface.
 */
@InterfaceAudience.Private
public interface AMContext {

  /**
   * Get application id
   * @return ApplicationId application id
   */
  ApplicationId getApplicationId();

  /**
   * Get application attempt id
   * @return ApplicationAttemptId application attempt id
   */
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * Get application name
   * @return String application name
   */
  String getApplicationName();

  /**
   * Get application start timestamp
   * @return long application start timestamp
   */
  long getStartTime();

  /**
   * Get global event handler in master
   * @return EventHandler global event handler in master
   */
  @SuppressWarnings("rawtypes")
  EventHandler getEventHandler();

  /**
   * Get system clock
   * @return Clock system clock
   */
  Clock getClock();

  /**
   * Get Client to Master token secret manager
   * @return ClientToAMTokenSecretManager Client to Master token secret manager
   */
  ClientToAMTokenSecretManager getClientToAMTokenSecretManager();

  /**
   * Get the name of user that submit the application
   * @return String the name of user that submit the application
   */
  String getUser();

  /**
   * Get yarn credentials
   * @return Credentials yarn credentials
   */
  Credentials getCredentials();

  /**
   * Get parameter server manager
   * @return ParameterServerManager parameter server manager
   */
  ParameterServerManager getParameterServerManager();

  /**
   * Get container allocator
   * @return ContainerAllocator container allocator
   */
  ContainerAllocator getContainerAllocator();

  /**
   * Get rpc server
   * @return MasterService rpc server
   */
  MasterService getMasterService();

  /**
   * Get event dispatcher
   * @return event dispatcher
   */
  Dispatcher getDispatcher();

  /**
   * Get application state manager
   * @return App application state manager
   */
  App getApp();

  /**
   * Get application configuration
   * @return Configuration application configuration
   */
  Configuration getConf();

  /**
   * Get web server
   * @return WebApp web server
   */
  WebApp getWebApp();

  /**
   * Get matrix meta manager
   * @return MatrixMetaManager matrix meta manager
   */
  AMMatrixMetaManager getMatrixMetaManager();

  /**
   * Get ps location manager
   * @return LocationManager ps location manager
   */
  LocationManager getLocationManager();

  /**
   * Get running mode, ANGEL_PS_WORKER or ANGEL_PS
   * @return RunningMode running mode
   */
  RunningMode getRunningMode();

  /**
   * Get PSAgent manager
   * @return PSAgentManager PSAgent manager
   */
  PSAgentManager getPSAgentManager();

  /**
   * Get Worker manager
   * @return WorkerManager Worker manager
   */
  WorkerManager getWorkerManager();

  /**
   * Get train data splitter
   * @return DataSpliter train data splitter
   */
  DataSpliter getDataSpliter();

  /**
   * Get total train iteration number
   * @return int total train iteration number
   */
  int getTotalIterationNum();
  
  /**
   * Get task manager
   * @return AMTaskManager task manager
   */
  AMTaskManager getTaskManager();
  
  /**
   * Get master attempt index
   * @return int master attempt index
   */
  int getAMAttemptTime();

  /**
   * Get application state storage
   * @return application state storage
   */
  AppStateStorage getAppStateStorage();
  
  /**
   * Is we should clean the resource of the application
   * @return boolean true means need clean
   */
  boolean needClear();

  /**
   * Get application deploy mode:LOCAL or YARN
   * @return AngelDeployMode application deploy mode
   */
  AngelDeployMode getDeployMode();

  /**
   * Get algorithm log collector
   * @return AlgoLogService
   */
  MetricsService getAlgoMetricsService();

  /**
   * Get Matrix partition Replication number
   * @return Matrix partition Replication number
   */
  int getPSReplicationNum();

  /**
   * Get Client Manager
   * @return Angel client manager
   */
  ClientManager getClientManager();

  /**
   * Get Yarn web port
   * @return Yarn web port
   */
  int getYarnNMWebPort();
}
