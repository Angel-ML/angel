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
 * The container request and assign have been modified according to the specific
 * circumstances of Angel.
 */

package com.tencent.angel.master.deploy.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.App;
import com.tencent.angel.master.app.AppState;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptContainerAssignedEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptContainerAssignedEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.psagent.PSAgentAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Container allocator on Yarn deploy mode. It is responsible for communicating with Yarn's RM,
 * requesting resources or releasing resources from the RM and receiving container exit
 * information
 */
public class YarnContainerAllocator extends ContainerAllocator {

  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  private final AMContext context;
  
  /**stop service or not*/
  private final AtomicBoolean stopped;
  
  /**event handler thread.*/
  private Thread eventHandlingThread;
  
  /**event queue*/
  BlockingQueue<ContainerAllocatorEvent> eventQueue =
      new LinkedBlockingQueue<ContainerAllocatorEvent>();

  /**Request record factory*/
  private final RecordFactory recordFactory;
  
  /**last response id from RM*/
  private int lastResponseId;
  
  /**the timestamp of last heartbeat*/
  private long lastHeartbeatTime;

  /**rm communication proxy*/
  private ApplicationMasterProtocol amRmProtocol;
  
  /**heartbeat thread*/
  private Thread allocatorThread;
  
  /**heartbeat interval*/
  private int rmPollInterval;

  /**whether to successfully unregister from the Yarn RM*/
  protected AtomicBoolean successfullyUnregistered = new AtomicBoolean(false);

  /**resource request table*/
  private final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>(
      new org.apache.hadoop.yarn.api.records.ResourceRequest.ResourceRequestComparator());
  
  /**need release container id set*/
  private final Set<ContainerId> release = new TreeSet<ContainerId>();

  /**resource priority->Map(Module Id->container request), used to save each worker or ps resource request */
  private final Map<Priority, Map<Id, ContainerRequest>> idToRequestMaps =
      new HashMap<Priority, Map<Id, ContainerRequest>>();
  
  /**resource priority->Map(host->Module Ids), used to save components that are intended to be started on the host*/
  private final Map<Priority, Map<String, LinkedList<Id>>> hostToIDListMaps =
      new HashMap<Priority, Map<String, LinkedList<Id>>>();
  
  /**resource priority->Map(rack->Module Ids), used to save components that are intended to be started on the rack*/
  private final Map<Priority, Map<String, LinkedList<Id>>> rackToIDListMaps =
      new HashMap<Priority, Map<String, LinkedList<Id>>>();

  /**resource priority->Map(host->Map(resource->resource request)), used to save the resource request to each host*/
  private final Map<Priority, Map<String, Map<Resource, ResourceRequest>>> remoteRequestsTable =
      new TreeMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();

  /**statistics the number of containers assigned*/
  private int containersAllocated;
  
  /**statistics the number of containers released*/
  private int containersReleased;
  
  /**statistics the number of containers which are host data local*/
  private int hostLocalAssigned;
  
  /**statistics the number of containers which are rack data local*/
  private int rackLocalAssigned;
  
  /**container id to module id map*/
  private final Map<ContainerId, Id> assignedContainerToIDMap = new HashMap<ContainerId, Id>();
  
  /**module id to container id map*/
  private final Map<Id, Container> idToContainerMap = new HashMap<Id, Container>();
  
  private Map<ApplicationAccessType, String> applicationACLs;
  private final Lock readLock;
  private final Lock writeLock;

  public YarnContainerAllocator(AMContext context) {
    super(YarnContainerAllocator.class.getName());
    this.context = context;
    this.recordFactory = RecordFactoryProvider.getRecordFactory(null);
    stopped = new AtomicBoolean(false);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  @Override
  protected void serviceStart() throws Exception {
    startEventHandlerThread();
    startRMComminicatorThread();
    super.serviceStart();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.amRmProtocol = ClientRMProxy.createRMProxy(getConfig(), ApplicationMasterProtocol.class);
    this.rmPollInterval =
        conf.getInt(AngelConf.ANGEL_AM_HEARTBEAT_INTERVAL_MS,
            AngelConf.DEFAULT_ANGEL_AM_HEARTBEAT_INTERVAL_MS);
    RackResolver.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("InterruptedException while stopping");
      }
    }

    if (allocatorThread != null) {
      allocatorThread.interrupt();
      try {
        allocatorThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("InterruptedException while stopping");
      }
    }

    //If we need Yarn to restart a new application master, we should not unregister from Yarn RM
    if(context.needClear()) {
      unregister();
    }
    
    super.serviceStop();
    LOG.info("ContainerAllocator service stop!");
  }

  private void startRMComminicatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        //register to Yarn RM
        try {
          register();
          LOG.info("register to rm success");
        } catch (Exception e) {
          LOG.fatal("register am to rm failed. " + e);
          context.getEventHandler().handle(
              new InternalErrorEvent(context.getApplicationId(), "register am to rm failed. " + e.getMessage()));
          return;
        }

        //send heartbeat to Yarn RM every rmPollInterval milliseconds
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            try {
              heartbeat();
            } catch (YarnRuntimeException e) {
              //catch YarnRuntimeException, we should exit and need not retry
              LOG.fatal("send heartbeat to rm failed. " + e);
              context.getEventHandler().handle(
                  new InternalErrorEvent(context.getApplicationId(), "send heartbeat to rm failed. " + e.getMessage(), false));
              return;
            } catch (Exception e) {
              LOG.error("send heartbeat to rm failed. ", e);
              continue;
            }

            try{
              writeLock.lock();
              lastHeartbeatTime = context.getClock().getTime();
            } finally {
              writeLock.unlock();
            }            
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.fatal("allocator thread interrupted. ", e);
              context.getEventHandler().handle(
                  new InternalErrorEvent(context.getApplicationId(), "allocator thread interrupted. " + e.getMessage()));
            }
            return;
          }
        }
      }
    });
    allocatorThread.setName("RMCommunicator Allocator");
    allocatorThread.start();
  }

  private void heartbeat() throws IOException {
    try {
      writeLock.lock();
      ResourceBlacklistRequest blacklistRequest =
          ResourceBlacklistRequest.newInstance(new ArrayList<String>(),
              new ArrayList<String>());
      
      for(ResourceRequest request : ask) {
        LOG.info("ask request=" + request);
      }
      
      //build heartbeat request
      AllocateRequest allocateRequest =
          AllocateRequest.newInstance(lastResponseId, 0.5f, new ArrayList<ResourceRequest>(ask),
              new ArrayList<ContainerId>(release), blacklistRequest);

      LOG.debug("heartbeat, allocateRequest = " + allocateRequest.toString());

      //send heartbeat request to rm
      AllocateResponse allocateResponse;
      try {
        allocateResponse = amRmProtocol.allocate(allocateRequest);
        LOG.debug("heartbeat, allocateResponse = " + allocateResponse.toString());
      } catch (YarnException e) {
        throw new IOException(e);
      }

      lastResponseId = allocateResponse.getResponseId();

      if (allocateResponse.getAMCommand() != null) {
        switch (allocateResponse.getAMCommand()) {
          case AM_RESYNC:
          case AM_SHUTDOWN:
            // This can happen if the RM has been restarted. If it is in that state,
            // this application must clean itself up.
            throw new YarnRuntimeException("Resource Manager doesn't recognize AttemptId: "
                + this.context.getApplicationAttemptId());
          default:
            String msg = "Unhandled value of AMCommand: " + allocateResponse.getAMCommand();
            LOG.error(msg);
            throw new YarnRuntimeException(msg);
        }
      }

      List<Container> newContainers = allocateResponse.getAllocatedContainers();
      if (LOG.isDebugEnabled()) {
        printContainersInfo(newContainers);
      }

      // Setting NMTokens
      if (allocateResponse.getNMTokens() != null) {
        for (NMToken nmToken : allocateResponse.getNMTokens()) {
          NMTokenCache.setNMToken(nmToken.getNodeId().toString(), nmToken.getToken());
        }
      }

      ask.clear();
      release.clear();

      // assgin containers
      assignContainers(newContainers);

      // if some container is not assigned, release them
      containersNotAssigned(newContainers);

      //handle finish containers
      List<ContainerStatus> finishedContainers = allocateResponse.getCompletedContainersStatuses();
      handleFinishContainers(finishedContainers);
    } finally {
      writeLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private void handleFinishContainers(List<ContainerStatus> finishedContainers) {
    for (ContainerStatus cont : finishedContainers) {
      LOG.info("Received completed container:" + cont);
      Id id = assignedContainerToIDMap.get(cont.getContainerId());
      if (id == null) {
        LOG.error("Container complete event for unknown container id " + cont.getContainerId());
      } else {
        assignedContainerToIDMap.remove(cont.getContainerId());
        idToContainerMap.remove(id);
        
        //dispatch container exit message to corresponding components
        String diagnostics = StringInterner.weakIntern(cont.getDiagnostics());
        if (id instanceof PSAttemptId) {
          context.getEventHandler().handle(
              new PSAttemptDiagnosticsUpdateEvent(diagnostics, (PSAttemptId) id));
          context.getEventHandler().handle(createContainerFinishedEvent(cont, (PSAttemptId) id));
        } else if(id instanceof WorkerAttemptId){
          context.getEventHandler().handle(
              new WorkerAttemptDiagnosticsUpdateEvent((WorkerAttemptId) id, diagnostics));
          context.getEventHandler().handle(
              createContainerFinishedEvent(cont, (WorkerAttemptId) id));
        }
      }
    }
  }

  private PSAttemptEvent createContainerFinishedEvent(ContainerStatus cont, PSAttemptId psAttemptId) {
    if (cont.getExitStatus() == ContainerExitStatus.ABORTED) {
      // killed by framework
      return new PSAttemptEvent(PSAttemptEventType.PA_KILL, psAttemptId);
    } else {
      return new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_COMPLETE, psAttemptId);
    }
  }
   
  private WorkerAttemptEvent createContainerFinishedEvent(ContainerStatus cont,
      WorkerAttemptId attemptId) {
    if (cont.getExitStatus() == ContainerExitStatus.ABORTED) {
      // killed by framework
      return new WorkerAttemptEvent(WorkerAttemptEventType.KILL, attemptId);
    } else {
      return new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_COMPLETE,
          attemptId);
    }
  }

  private void printContainersInfo(List<Container> newContainers) {
    if (newContainers != null) {
      for (Container c : newContainers) {
        LOG.debug("finish" + c.toString());
      }
    }
  }

  private void register() throws YarnException, IOException {
    RegisterApplicationMasterRequest request =
        recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);

    InetSocketAddress bindAddress = context.getMasterService().getRPCListenAddr();
    if (bindAddress == null) {
      LOG.debug("bindAddress is null");
    }
    request.setHost(bindAddress.getAddress().getHostAddress());
    request.setRpcPort(bindAddress.getPort());
    request.setTrackingUrl("http://" + bindAddress.getAddress().getHostAddress() + ":"
        + context.getWebApp().port());

    RegisterApplicationMasterResponse response = amRmProtocol.registerApplicationMaster(request);

    setApplicationACLs(response.getApplicationACLs());

    LOG.info("register am over");
    LOG.info("MaximumResourceCapability = " + response.getMaximumResourceCapability());
  }

  protected void unregister() {
    try {
      LOG.info("to unregister from Yarn RM");
      doUnregistration();
    } catch (Exception are) {
      LOG.error("Exception while unregistering ", are);
    }
  }

  @VisibleForTesting
  protected void doUnregistration() throws YarnException, IOException, InterruptedException {
    FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
    App app = context.getApp();

    //get application finish state
    AppState appState = app.getInternalState();
    if (appState == AppState.SUCCEEDED) {
      finishState = FinalApplicationStatus.SUCCEEDED;
    } else if (appState == AppState.KILLED) {
      finishState = FinalApplicationStatus.KILLED;
    } else if (appState == AppState.FAILED) {
      finishState = FinalApplicationStatus.FAILED;
    }
    
    //build application diagnostics
    StringBuilder sb = new StringBuilder();
    for (String s : app.getDiagnostics()) {
      sb.append(s).append("\n");
    }
    LOG.info("Setting job diagnostics to " + sb.toString());

    //TODO:add a job history for angel
    String historyUrl = "angel does not have history url now";
    
    //build unregister request
    FinishApplicationMasterRequest request =
        FinishApplicationMasterRequest.newInstance(finishState, sb.toString(), historyUrl);
    
    //send unregister request to rm
    while (true) {
      try {
        FinishApplicationMasterResponse response = amRmProtocol.finishApplicationMaster(request);
        if (response.getIsUnregistered()) {
          break;
        }
      } catch (Exception x) {
        LOG.error("unregister failed ", x);
        break;
      }

      LOG.info("Waiting for application to be successfully unregistered.");
      Thread.sleep(rmPollInterval);
    }

    successfullyUnregistered.set(true);
  }

  public boolean hasSuccessfullyUnregistered() {
    return successfullyUnregistered.get();
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected void handleEvent(YarnContainerAllocatorEvent event) {
    try {
      writeLock.lock();
      switch (event.getType()) {
        case CONTAINER_REQ: 
          requestContainer((YarnContainerRequestEvent) event);
          break;

        case CONTAINER_DEALLOCATE:
          deallocateContainer(event);
          break;
          
        default:
          break;
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void deallocateContainer(YarnContainerAllocatorEvent event) {
    Container c = idToContainerMap.get(event.getTaskId());
    Map<Id, ContainerRequest> idToRequestMap = idToRequestMaps.get(event.getPriority());
    if (idToRequestMap == null) {
      return;
    }

    ContainerRequest request = idToRequestMap.get(event.getTaskId());
    if (c == null && request != null) {
      decContainerReq(request);
    } else {
      idToContainerMap.remove(event.getTaskId());
      containersReleased++;
      release.add(c.getId());
    }
  }

  private void requestContainer(YarnContainerRequestEvent event) {
    String[] hosts = event.getHosts();

    Set<String> racks = null;
    if (hosts != null && hosts.length > 0) {
      racks = new HashSet<String>();
      for (String host : hosts) {
        racks.add(RackResolver.resolve(host).getNetworkLocation());
      }
    }

    ContainerRequest newRequest =
        new ContainerRequest(event.getTaskId(), event.getResource(), hosts, racks == null ? null
            : racks.toArray(new String[0]), event.getPriority());

    addResourceRequest(event.getTaskId(), newRequest);
  }

  private void startEventHandlerThread() {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        YarnContainerAllocatorEvent event;

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = (YarnContainerAllocatorEvent) eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.fatal("yarn allocator event handler is interrupted. ", e);
              context.getEventHandler().handle(
                  new InternalErrorEvent(context.getApplicationId(), "yarn allocator event handler is interrupted. " + e.getMessage()));
            }
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.fatal("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);

            context.getEventHandler().handle(
                new InternalErrorEvent(context.getApplicationId(),"Error in handling event type " + event.getType()
                    + " to the ContainreAllocator" + t.getMessage()));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
  }

  private void addResourceRequest(Id id, ContainerRequest request) {
    Map<Id, ContainerRequest> idToRequestMap = idToRequestMaps.get(request.priority);
    if (idToRequestMap == null) {
      idToRequestMap = new HashMap<Id, ContainerRequest>();
      idToRequestMaps.put(request.priority, idToRequestMap);
    }
    idToRequestMap.put(id, request);
    if (request.hosts != null && request.hosts.length > 0) {
      Map<String, LinkedList<Id>> hostToIDListMap = hostToIDListMaps.get(request.priority);
      if (hostToIDListMap == null) {
        hostToIDListMap = new HashMap<String, LinkedList<Id>>();
        hostToIDListMaps.put(request.priority, hostToIDListMap);
      }

      for (String host : request.hosts) {
        LinkedList<Id> idList = hostToIDListMap.get(host);
        if (idList == null) {
          idList = new LinkedList<Id>();
          hostToIDListMap.put(host, idList);
        }
        idList.add(id);         
        addResourceRequest(request.priority, host, request.capability);
      }
    }

    if (request.racks != null && request.racks.length > 0) {
      Map<String, LinkedList<Id>> rackToIDListMap = rackToIDListMaps.get(request.priority);
      if (rackToIDListMap == null) {
        rackToIDListMap = new HashMap<String, LinkedList<Id>>();
        hostToIDListMaps.put(request.priority, rackToIDListMap);
      }

      for (String rack : request.racks) {
        LinkedList<Id> idList = rackToIDListMap.get(rack);
        if (idList == null) {
          idList = new LinkedList<Id>();
          rackToIDListMap.put(rack, idList);
        }
        idList.add(id);
        addResourceRequest(request.priority, rack, request.capability);
      }
    }

    addResourceRequest(request.priority, ResourceRequest.ANY, request.capability);
  }

  private void addResourceRequest(Priority priority, String resourceName, Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
        this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, Map<Resource, ResourceRequest>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + priority);
      }
    }
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new HashMap<Resource, ResourceRequest>();
      remoteRequests.put(resourceName, reqMap);
    }
    ResourceRequest remoteRequest = reqMap.get(capability);
    if (remoteRequest == null) {
      remoteRequest = recordFactory.newRecordInstance(ResourceRequest.class);
      remoteRequest.setPriority(priority);
      remoteRequest.setResourceName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      reqMap.put(capability, remoteRequest);
    }
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);

    // Note this down for next interaction with ResourceManager
    addResourceRequestToAsk(remoteRequest);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest:" + " applicationId=" + context.getApplicationId()
          + " priority=" + priority.getPriority() + " resourceName=" + resourceName
          + " numContainers=" + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }

  private void addResourceRequestToAsk(ResourceRequest remoteRequest) {
    if (ask.contains(remoteRequest)) {
      ask.remove(remoteRequest);
    }
    ask.add(remoteRequest);
  }

  private void decResourceRequest(Priority priority, String resourceName, Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
        this.remoteRequestsTable.get(priority);
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not decrementing resource as " + resourceName
            + " is not present in request table");
      }
      return;
    }
    ResourceRequest remoteRequest = reqMap.get(capability);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEFORE decResourceRequest:" + " applicationId=" + context.getApplicationId()
          + " priority=" + priority.getPriority() + " resourceName=" + resourceName
          + " numContainers=" + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }

    if (remoteRequest.getNumContainers() > 0) {
      // based on blacklisting comments above we can end up decrementing more
      // than requested. so guard for that.
      remoteRequest.setNumContainers(remoteRequest.getNumContainers() - 1);
    }

    if (remoteRequest.getNumContainers() == 0) {
      reqMap.remove(capability);
      if (reqMap.size() == 0) {
        remoteRequests.remove(resourceName);
      }
      if (remoteRequests.size() == 0) {
        remoteRequestsTable.remove(priority);
      }
    }

    // send the updated resource request to RM
    // send 0 container count requests also to cancel previous requests
    addResourceRequestToAsk(remoteRequest);

    if (LOG.isDebugEnabled()) {
      LOG.info("AFTER decResourceRequest:" + " applicationId=" + context.getApplicationId()
          + " priority=" + priority.getPriority() + " resourceName=" + resourceName
          + " numContainers=" + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }

  private void containersNotAssigned(List<Container> containers) {
    containersReleased += containers.size();
    int size = containers.size();
    for (int i = 0; i < size; i++) {
      LOG.info("need release container=" + containers.get(i));
      release.add(containers.get(i).getId());
    }
  }

  private void decContainerReq(ContainerRequest req) {
    // Update resource requests
    if (req.hosts != null) {
      for (String hostName : req.hosts) {
        decResourceRequest(req.priority, hostName, req.capability);
      }
    }

    if (req.id instanceof PSAgentAttemptId) {
      return;
    }

    if (req.racks != null) {
      for (String rack : req.racks) {
        decResourceRequest(req.priority, rack, req.capability);
      }
    }

    decResourceRequest(req.priority, ResourceRequest.ANY, req.capability);
  }

  private void assignContainers(List<Container> allocatedContainers) {
    // try to assign to all nodes first to match node local
    Iterator<Container> it = allocatedContainers.iterator();

    while (it.hasNext()) {
      Container allocated = it.next();
      Map<String, LinkedList<Id>> hostToIDListMap = hostToIDListMaps.get(allocated.getPriority());
      Map<Id, ContainerRequest> idToRequestMap = idToRequestMaps.get(allocated.getPriority());
      if (hostToIDListMap == null || hostToIDListMap.isEmpty() || idToRequestMap == null
          || idToRequestMap.isEmpty()) {
        continue;
      }

      String host = allocated.getNodeId().getHost();
      LinkedList<Id> list = hostToIDListMap.get(host);
      while (list != null && list.size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Host matched to the request list " + host);
        }
        Id tId = list.removeFirst();
        if (idToRequestMap.containsKey(tId)) {
          ContainerRequest assigned = idToRequestMap.remove(tId);
          containerAssigned(allocated, assigned);
          it.remove();

          hostLocalAssigned++;
          LOG.debug("Assigned based on host match " + host);
          break;
        }
      }
    }

    // try to match all rack local
    it = allocatedContainers.iterator();
    while (it.hasNext()) {
      Container allocated = it.next();
      Map<String, LinkedList<Id>> rackToIDListMap = rackToIDListMaps.get(allocated.getPriority());
      Map<Id, ContainerRequest> idToRequestMap = idToRequestMaps.get(allocated.getPriority());
      if (rackToIDListMap == null || rackToIDListMap.isEmpty() || idToRequestMap == null
          || idToRequestMap.isEmpty()) {
        continue;
      }

      String host = allocated.getNodeId().getHost();
      String rack = RackResolver.resolve(host).getNetworkLocation();

      LinkedList<Id> list = rackToIDListMap.get(rack);
      while (list != null && list.size() > 0) {
        Id tId = list.removeFirst();
        if (idToRequestMap.containsKey(tId)) {
          ContainerRequest assigned = idToRequestMap.remove(tId);
          containerAssigned(allocated, assigned);
          it.remove();
          rackLocalAssigned++;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Assigned based on rack match " + rack);
          }
          break;
        }
      }
    }

    // assign remaining
    it = allocatedContainers.iterator();
    while (it.hasNext()) {
      Container allocated = it.next();
      Map<Id, ContainerRequest> idToRequestMap = idToRequestMaps.get(allocated.getPriority());
      Id tId = idToRequestMap.keySet().iterator().next();
      ContainerRequest assigned = idToRequestMap.remove(tId);
      containerAssigned(allocated, assigned);
      it.remove();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Assigned based on * match");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void containerAssigned(Container allocated, ContainerRequest assigned) {
    // Update resource requests
    decContainerReq(assigned);

    // send the container-assigned event to task attempt
    if (assigned.id instanceof PSAttemptId) {
      context.getEventHandler().handle(
          new PSAttemptContainerAssignedEvent((PSAttemptId) assigned.id, allocated));
    } else if (assigned.id instanceof WorkerAttemptId) {
      context.getEventHandler().handle(
          new WorkerAttemptContainerAssignedEvent((WorkerAttemptId) assigned.id, allocated));
    }

    assignedContainerToIDMap.put(allocated.getId(), assigned.id);
    idToContainerMap.put(assigned.id, allocated);

    LOG.info("Assigned container (" + allocated + ") " + " to task " + assigned.id + " on node "
        + allocated.getNodeId().toString());
  }

  public Map<ApplicationAccessType, String> getApplicationACLs() {
    return applicationACLs;
  }

  public void setApplicationACLs(Map<ApplicationAccessType, String> applicationACLs) {
    this.applicationACLs = applicationACLs;
  }

  public void forceSuccessfullyUnregistered() {
    successfullyUnregistered.set(true);
  }

  public int getLastResponseId() {
    try {
      readLock.lock();
      return lastResponseId;
    } finally {
      readLock.unlock();
    }
  }

  public long getLastHeartbeatTime() {
    try {
      readLock.lock();
      return lastHeartbeatTime;
    } finally {
      readLock.unlock();
    }
  }

  public int getContainersAllocated() {
    try {
      readLock.lock();
      return containersAllocated;
    } finally {
      readLock.unlock();
    }
  }

  public int getContainersReleased() {
    try {
      readLock.lock();
      return containersReleased;
    } finally {
      readLock.unlock();
    }
  }

  public int getRackLocalAssigned() {
    try {
      readLock.lock();
      return rackLocalAssigned;
    } finally {
      readLock.unlock();
    }
  }

  public int getHostLocalAssigned() {
    try {
      readLock.lock();
      return hostLocalAssigned;
    } finally {
      readLock.unlock();
    }
  }
}
