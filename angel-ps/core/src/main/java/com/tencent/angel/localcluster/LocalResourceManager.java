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

package com.tencent.angel.localcluster;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Local resource manager. It startups a event handler to handle allocation/deallocation events.
 */
public class LocalResourceManager {
  private static final Log LOG = LogFactory.getLog(LocalResourceManager.class);
  
  /**master attempt indexes*/
  private final Map<ApplicationId, Integer> appIdToAttemptIndexMap;
  
  /**event queue*/
  private final LinkedBlockingQueue<LocalRMEvent> eventQueue;
  
  /**Is stop the event handler*/
  private final AtomicBoolean stopped;
  
  /**master maximum attempt number*/
  private final int maxAttemptNum;
  
  /**event handler*/
  private Thread handler;
  
  /**
   * Create a local resource manager
   */
  public LocalResourceManager(Configuration conf) {
    appIdToAttemptIndexMap = new HashMap<ApplicationId, Integer>();
    eventQueue = new LinkedBlockingQueue<LocalRMEvent>();
    stopped = new AtomicBoolean(false);
    maxAttemptNum = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
  }
  
  /**
   * Event handler thread.
   */
  private class Handler extends Thread {
    @Override
    public void run() {
      while(!stopped.get() && !Thread.interrupted()) {
        LocalRMEvent event = null;
        try {
          event = eventQueue.take();
          LOG.info("local rm handle a event " + event);
          switch(event.getType()) {
            case ALLOCATE:
            case FAILED:
              allocate(event);
              break;
          }
        } catch (InterruptedException e) {
          LOG.warn("event handler is interrupted");
        }
        catch(IOException | InvalidParameterException e){
          LOG.error("handle event " + event + " failed.", e);
        }
      }
    }
  }
  
  private void allocate(LocalRMEvent event) throws IOException, InvalidParameterException {
    ApplicationId appId = event.getAppId();
    int attemptIndex;
    if(appIdToAttemptIndexMap.containsKey(appId)) {
      attemptIndex = appIdToAttemptIndexMap.get(appId);     
    } else {
      attemptIndex = 1;
    }
    
    appIdToAttemptIndexMap.put(appId, attemptIndex + 1);
    
    if(attemptIndex > maxAttemptNum) {
      return;
    }
    LocalClusterContext.get().setMaster(null);
    stopWorkerAndPS();
    
    LocalMaster master = new LocalMaster(ApplicationAttemptId.newInstance(appId, attemptIndex));
    master.start();
    LocalClusterContext.get().setMaster(master);
  }
  
  private void stopWorkerAndPS() {
    Map<WorkerAttemptId, LocalWorker> localWorkers = LocalClusterContext.get().getIdToWorkerMap();
    for(LocalWorker localWorker:localWorkers.values()) {
      localWorker.exit();
    }
    
    Map<PSAttemptId, LocalPS> localPSs = LocalClusterContext.get().getIdToPSMap();
    for(LocalPS localPS:localPSs.values()) {
      localPS.exit();
    }
  }
  
  /**
   * Allocate a master
   * @param appId application id
   */
  public void allocateMaster(ApplicationId appId) {
    try {
      eventQueue.put(new LocalRMEvent(appId, LocalRMEventType.ALLOCATE));
    } catch (InterruptedException e) {
      LOG.warn("waiting for add element to queue interupted.");
    }
  }
 
  /**
   * Master exit
   * @param appId application id
   */
  public void masterExited(ApplicationId appId) {
    try {
      eventQueue.put(new LocalRMEvent(appId, LocalRMEventType.FAILED));
    } catch (InterruptedException e) {
      LOG.warn("waiting for add element to queue interupted.");
    }
  }
  
  /**
   * Start event handler
   */
  public void start() {
    handler = new Handler();
    handler.setName("local-rm-handler");
    handler.start();
  }
  
  /**
   * Stop event hanlder
   */
  public void stop() {
    stopped.set(true);
    handler.interrupt();
    
    try {
      handler.join();
    } catch (InterruptedException ie) {
      LOG.warn("InterruptedException while stopping", ie);
    }
  }
}
