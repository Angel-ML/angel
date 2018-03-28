/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The container launch and launch result notify have been modified according to the specific
 * circumstances of Angel.
 */
package com.tencent.angel.master.deploy.yarn;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Yarn container launcher.It is responsible for interacting with Yarn's NodeManagers, starting or killing the container
 */
public class YarnContainerLauncher extends ContainerLauncher {
  static final Log LOG = LogFactory.getLog(ContainerLauncher.class);
  protected static final int INITIAL_POOL_SIZE = 10;

  private final ConcurrentHashMap<ContainerId, Container> containers;
  private final AMContext context;
  protected ThreadPoolExecutor launcherPool;
  
  private int limitOnPoolSize;
  private Thread eventHandlingThread;
  protected final BlockingQueue<ContainerLauncherEvent> eventQueue;
  private final AtomicBoolean stopped;
  private ContainerManagementProtocolProxy cmProxy;

  private Container getContainer(YarnContainerLauncherEvent event) {
    ContainerId id = event.getContainerId();
    Container c = containers.get(id);
    if (c == null) {
      c = new Container(event.getId(), event.getContainerId(), event.getContainerMgrAddress());
      Container old = containers.putIfAbsent(id, c);
      if (old != null) {
        c = old;
      }
    }
    return c;
  }

  private void removeContainerIfDone(ContainerId id) {
    Container c = containers.get(id);
    if (c != null && c.isCompletelyDone()) {
      containers.remove(id);
    }
  }

  private static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  private class Container {
    private ContainerState state;
    private Id taskId;
    private ContainerId containerId;
    final private String containerMgrAddress;

    public Container(Id taskId, ContainerId containerId, String containerMgrAddress) {
      this.state = ContainerState.PREP;
      this.taskId = taskId;
      this.containerMgrAddress = containerMgrAddress;
      this.containerId = containerId;
    }

    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }

    public synchronized void launch(ContainerRemoteLaunchEvent event) {
      LOG.info("Launching " + taskId);
      if (this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        notifyContainerLaunchFailed(event, "Container was killed before it was launched");
        return;
      }

      ContainerManagementProtocolProxyData proxy = null;
      try {

        proxy = getCMProxy(containerMgrAddress, containerId);

        //build the start container request use launch context 
        ContainerLaunchContext containerLaunchContext = event.getContainerLaunchContext();
        StartContainerRequest startRequest =
            StartContainerRequest.newInstance(containerLaunchContext, event.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(startRequest);
        StartContainersRequest requestList = StartContainersRequest.newInstance(list);
        
        //send the start request to Yarn nm
        StartContainersResponse response =
            proxy.getContainerManagementProtocol().startContainers(requestList);
        if (response.getFailedRequests() != null
            && response.getFailedRequests().containsKey(containerId)) {
          throw response.getFailedRequests().get(containerId).deSerialize();
        }

        //send the message that the container starts successfully to the corresponding component
        notifyContainerLaunchSuccess(event);
        
        // after launching, send launched event to task attempt to move
        // it from ASSIGNED to RUNNING state        
        this.state = ContainerState.RUNNING;
      } catch (Throwable t) {
        String message =
            "Container launch failed for " + containerId + " : "
                + StringUtils.stringifyException(t);
        LOG.error(message);
        this.state = ContainerState.FAILED;
        
        //send the message that the container starts failed to the corresponding component
        notifyContainerLaunchFailed(event, message);
      } finally {
        if (proxy != null) {
          cmProxy.mayBeCloseProxy(proxy);
        }
      }
    }

    @SuppressWarnings("unchecked")
    private void notifyContainerLaunchSuccess(ContainerRemoteLaunchEvent event) {
      Id id = event.getId();
      if (id instanceof PSAttemptId) {
        context.getEventHandler().handle(
            new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCHED, (PSAttemptId) id));
      } else if (id instanceof WorkerAttemptId) {
        context.getEventHandler().handle(
            new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCHED, (WorkerAttemptId) id));
      }
    }

    @SuppressWarnings("unchecked")
    private void notifyContainerLaunchFailed(ContainerRemoteLaunchEvent event, String message) {
      Id id = event.getId();
      if (id instanceof PSAttemptId) {
        context.getEventHandler().handle(
            new PSAttemptDiagnosticsUpdateEvent(message, (PSAttemptId) id));
        context.getEventHandler().handle(
            new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED, (PSAttemptId) id));
      } else if (id instanceof WorkerAttemptId) {
        context.getEventHandler().handle(
            new WorkerAttemptDiagnosticsUpdateEvent((WorkerAttemptId) id, message));
        context.getEventHandler().handle(
            new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCH_FAILED, (WorkerAttemptId) id));
      }
    }

    public synchronized void kill() {

      if (this.state == ContainerState.PREP) {
        this.state = ContainerState.KILLED_BEFORE_LAUNCH;
      } else if (!isCompletelyDone()) {
        LOG.info("KILLING " + taskId);

        ContainerManagementProtocolProxyData proxy = null;
        try {
          proxy = getCMProxy(this.containerMgrAddress, this.containerId);

          // kill the remote container if already launched
          List<ContainerId> ids = new ArrayList<ContainerId>();
          ids.add(this.containerId);
          StopContainersRequest request = StopContainersRequest.newInstance(ids);
          StopContainersResponse response =
              proxy.getContainerManagementProtocol().stopContainers(request);
          if (response.getFailedRequests() != null
              && response.getFailedRequests().containsKey(this.containerId)) {
            throw response.getFailedRequests().get(this.containerId).deSerialize();
          }
          LOG.info("stop container success, containerMgrAddress:" + containerMgrAddress);
        } catch (Throwable t) {
          String message =
              "cleanup failed for container " + this.containerId + " : "
                  + StringUtils.stringifyException(t);
          LOG.warn(message);
        } finally {
          if (proxy != null) {
            cmProxy.mayBeCloseProxy(proxy);
          }
        }
        this.state = ContainerState.DONE;
      }
    }
  }

  public YarnContainerLauncher(AMContext context) {
    super(YarnContainerLauncher.class.getName());
    this.context = context;
    this.stopped = new AtomicBoolean(false);
    this.containers = new ConcurrentHashMap<ContainerId, Container>();
    this.eventQueue = new LinkedBlockingQueue<ContainerLauncherEvent>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.limitOnPoolSize =
        conf.getInt(AngelConf.ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT,
            AngelConf.DEFAULT_ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT);
    LOG.info("Upper limit on the thread pool size is " + this.limitOnPoolSize);
    cmProxy = new ContainerManagementProtocolProxy(conf);
  }

  protected void serviceStart() throws Exception {

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("ContainerLauncher #%d").setDaemon(true).build();

    //start a thread pool to startup the container
    launcherPool =
        new ThreadPoolExecutor(INITIAL_POOL_SIZE, Integer.MAX_VALUE, 1, TimeUnit.HOURS,
            new LinkedBlockingQueue<Runnable>(), tf);
    
    eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        YarnContainerLauncherEvent event = null;
        Set<String> allNodes = new HashSet<String>();

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = (YarnContainerLauncherEvent) eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.fatal("yarn container launch event handler is interrupted. " + e);
              context.getEventHandler().handle(
                  new InternalErrorEvent(context.getApplicationId(), "yarn container launch event handler is interrupted. " + e.getMessage()));
            }
            return;
          }
          allNodes.add(event.getContainerMgrAddress());

          int poolSize = launcherPool.getCorePoolSize();

          // See if we need up the pool size only if haven't reached the
          // maximum limit yet.
          if (poolSize != limitOnPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int numNodes = allNodes.size();
            int idealPoolSize = Math.min(limitOnPoolSize, numNodes);

            if (poolSize < idealPoolSize) {
              // Bump up the pool size to idealPoolSize+INITIAL_POOL_SIZE, the
              // later is just a buffer so we are not always increasing the
              // pool-size
              int newPoolSize = Math.min(limitOnPoolSize, idealPoolSize + INITIAL_POOL_SIZE);
              LOG.info("Setting ContainerLauncher pool size to " + newPoolSize
                  + " as number-of-nodes to talk to is " + numNodes);
              launcherPool.setCorePoolSize(newPoolSize);
            }
          }

          // the events from the queue are handled in parallel
          // using a thread pool
          launcherPool.execute(createEventProcessor(event));
        }
      }
    };
    eventHandlingThread.setName("ContainerLauncher Event Handler");
    eventHandlingThread.start();
    super.serviceStart();
  }

  private void shutdownAllContainers() {
    for (Container ct : this.containers.values()) {
      if (ct != null) {
        ct.kill();
      }
    }
  }

  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    // shutdown any containers that might be left running
    shutdownAllContainers();
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (launcherPool != null) {
      launcherPool.shutdownNow();
    }
    super.serviceStop();
  }

  protected EventProcessor createEventProcessor(YarnContainerLauncherEvent event) {
    return new EventProcessor(event);
  }

  /**
   * Setup and start the container on remote nodemanager.
   */
  class EventProcessor implements Runnable {
    private YarnContainerLauncherEvent event;

    EventProcessor(YarnContainerLauncherEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("Processing the event " + event.toString());

      ContainerId containerId = event.getContainerId();
      Container c = getContainer(event);
      switch (event.getType()) {

        case CONTAINER_REMOTE_LAUNCH:
          ContainerRemoteLaunchEvent launchEvent = (ContainerRemoteLaunchEvent) event;
          c.launch(launchEvent);
          break;

        case CONTAINER_REMOTE_CLEANUP:
          c.kill();
          break;
      }
      removeContainerIfDone(containerId);
    }
  }

  @Override
  public void handle(ContainerLauncherEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  public ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData getCMProxy(
      String containerMgrBindAddr, ContainerId containerId) throws IOException {
    return cmProxy.getProxy(containerMgrBindAddr, containerId);
  }

  public AMContext getContext() {
    return context;
  }
}
