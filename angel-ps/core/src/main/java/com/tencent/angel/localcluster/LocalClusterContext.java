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

/**
 * Local cluster context. It contains all modules of Angel application.
 */
public class LocalClusterContext {
  /**local cluster*/
  private volatile LocalCluster localCluster;
  
  /**local master*/
  private volatile LocalMaster master;
  
  /**worker attempt id to local worker map*/
  private final Map<WorkerAttemptId, LocalWorker> idToWorkerMap;
  
  /**ps attempt id to local ps map*/
  private final Map<PSAttemptId, LocalPS> idToPSMap;
  
  /**application id*/
  private ApplicationId appId;
  
  /**local container id*/
  private ContainerId containerId;
  
  /**master host address*/
  private String localHost;
  
  /**master listening port*/
  private int port;
  
  /**master web listening port*/
  private int httpPort;
  
  /**cluster configuration*/
  private Configuration conf;
  
  private final static LocalClusterContext instance = new LocalClusterContext();
  
  /**
   * Create a new LocalClusterContext
   */
  private LocalClusterContext(){
    idToWorkerMap = new ConcurrentHashMap<WorkerAttemptId, LocalWorker>();
    idToPSMap = new ConcurrentHashMap<PSAttemptId, LocalPS>();
  }
  
  public static LocalClusterContext get(){
    return instance;
  }

  /**
   * Add a local worker
   * @param id worker attempt id
   * @param worker local worker
   */
  public void addWorker(WorkerAttemptId id, LocalWorker worker){
    idToWorkerMap.put(id, worker);
  }
  
  /**
   * Add a local ps
   * @param id ps attempt id
   * @param worker local ps
   */
  public void addPS(PSAttemptId id, LocalPS ps){
    idToPSMap.put(id, ps);
  }

  /**
   * Get local workers
   * @return local workers
   */
  public Map<WorkerAttemptId, LocalWorker> getIdToWorkerMap() {
    return idToWorkerMap;
  }

  /**
   * Get local pss
   * @return local pss
   */
  public Map<PSAttemptId, LocalPS> getIdToPSMap() {
    return idToPSMap;
  }

  /**
   * Get application id
   * @return application id
   */
  public ApplicationId getAppId() {
    return appId;
  }

  /**
   * Set application id
   * @param appId application id
   */
  public void setAppId(ApplicationId appId) {
    this.appId = appId;
  }

  /**
   * Get local container id
   * @return local container id
   */
  public ContainerId getContainerId() {
    return containerId;
  }

  /**
   * Set local container id
   * @param containerId local container id
   */
  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  /**
   * Get local master host
   * @return local master host
   */
  public String getLocalHost() {
    return localHost;
  }

  /**
   * Set local master host
   * @param localHost local master host
   */
  public void setLocalHost(String localHost) {
    this.localHost = localHost;
  }

  /**
   * Get master listening port
   * @return master listening port
   */
  public int getPort() {
    return port;
  }

  /**
   * Set master listening port
   * @param port master listening port
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Get master web port
   * @return master web port
   */
  public int getHttpPort() {
    return httpPort;
  }

  /**
   * Set master web port
   * @param httpPort master web port
   */
  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  /**
   * Get cluster configuration
   * @return cluster configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Set cluster configuration
   * @param conf cluster configuration
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Set local master
   * @param master local master
   */
  public void setMaster(LocalMaster master) {
    this.master = master;
  }

  /**
   * Get local master
   * @return local master
   */
  public LocalMaster getMaster() {
    return master;
  }
  
  /**
   * Get local ps use a ps attempt id
   * @param id ps attempt id
   * @return local ps
   */
  public LocalPS getPS(PSAttemptId id) {
    return idToPSMap.get(id);
  }

  /**
   * Get local worker use a worker attempt id
   * @param id worker attempt id
   * @return local worker
   */
  public LocalWorker getWorker(WorkerAttemptId id) {
    return idToWorkerMap.get(id);
  }

  /**
   * Clear all modules information
   */
  public void clear() {
    master = null;
    idToWorkerMap.clear();
    idToPSMap.clear();
  }

  /**
   * Get local cluster
   * @return local cluster
   */
  public LocalCluster getLocalCluster() {
    return localCluster;
  }

  /**
   * Set local cluster
   * @param localCluster local cluster
   */
  public void setLocalCluster(LocalCluster localCluster) {
    this.localCluster = localCluster;
  }
}
