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
import com.tencent.angel.master.LocationManager;
import com.tencent.angel.master.MasterService;
import com.tencent.angel.master.MatrixMetaManager;
import com.tencent.angel.master.data.DataSpliter;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.oplog.AppStateStorage;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.master.psagent.PSAgentManager;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.master.task.AMTaskManager;

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

@InterfaceAudience.Private
public interface AMContext {

  ApplicationId getApplicationId();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  long getStartTime();

  @SuppressWarnings("rawtypes")
  EventHandler getEventHandler();

  Clock getClock();

  ClientToAMTokenSecretManager getClientToAMTokenSecretManager();

  String getUser();

  Credentials getCredentials();

  ParameterServerManager getParameterServerManager();

  ContainerAllocator getContainerAllocator();

  MasterService getMasterService();

  Dispatcher getDispatcher();

  App getApp();

  Configuration getConf();

  WebApp getWebApp();

  MatrixMetaManager getMatrixMetaManager();

  LocationManager getLocationManager();

  RunningMode getRunningMode();

  PSAgentManager getPSAgentManager();

  WorkerManager getWorkerManager();

  DataSpliter getDataSpliter();

  int getTotalIterationNum();
  
  AMTaskManager getTaskManager();
  
  int getAMAttemptTime();

  AppStateStorage getAppStateStorage();
  
  boolean needClear();

  AngelDeployMode getDeployMode();
}
