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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;

public class LocalClusterContext {
  private volatile LocalCluster localCluster;
  private volatile LocalMaster master;
  private final Map<WorkerAttemptId, LocalWorker> idToWorkerMap;
  private final Map<PSAttemptId, LocalPS> idToPSMap;
  private ApplicationId appId;
  private ContainerId containerId;
  private String localHost;
  private int port;
  private int httpPort;
  private Configuration conf;
  
  private final static LocalClusterContext instance = new LocalClusterContext();
  
  private LocalClusterContext(){
    idToWorkerMap = new ConcurrentHashMap<WorkerAttemptId, LocalWorker>();
    idToPSMap = new ConcurrentHashMap<PSAttemptId, LocalPS>();
  }
  
  public static LocalClusterContext get(){
    return instance;
  }

  public void addWorker(WorkerAttemptId id, LocalWorker worker){
    idToWorkerMap.put(id, worker);
  }
  
  public void addPS(PSAttemptId id, LocalPS ps){
    idToPSMap.put(id, ps);
  }

  public Map<WorkerAttemptId, LocalWorker> getIdToWorkerMap() {
    return idToWorkerMap;
  }

  public Map<PSAttemptId, LocalPS> getIdToPSMap() {
    return idToPSMap;
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public void setAppId(ApplicationId appId) {
    this.appId = appId;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  public String getLocalHost() {
    return localHost;
  }

  public void setLocalHost(String localHost) {
    this.localHost = localHost;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public void setMaster(LocalMaster master) {
    this.master = master;
  }

  public LocalMaster getMaster() {
    return master;
  }
  
  public LocalPS getPS(PSAttemptId id) {
    return idToPSMap.get(id);
  }

  public LocalWorker getWorker(WorkerAttemptId id) {
    return idToWorkerMap.get(id);
  }

  public void clear() {
    master = null;
    idToWorkerMap.clear();
    idToPSMap.clear();
  }

  public LocalCluster getLocalCluster() {
    return localCluster;
  }

  public void setLocalCluster(LocalCluster localCluster) {
    this.localCluster = localCluster;
  }
}
