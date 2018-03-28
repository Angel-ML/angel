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

import com.google.protobuf.ServiceException;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentCommandProto;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentRegisterResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentReportResponse;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.client.PSControlClient;
import com.tencent.angel.psagent.client.PSControlClientManager;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.consistency.ConsistencyController;
import com.tencent.angel.psagent.executor.Executor;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.MatrixClientFactory;
import com.tencent.angel.psagent.matrix.PSAgentLocationManager;
import com.tencent.angel.psagent.matrix.PSAgentMatrixMetaManager;
import com.tencent.angel.psagent.matrix.cache.MatricesCache;
import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLogCache;
import com.tencent.angel.psagent.matrix.storage.MatrixStorageManager;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.adapter.MatrixClientAdapter;
import com.tencent.angel.utils.NetUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Agent for master and parameter servers, It fetches matrix meta from master, fetches matrices from
 * parameter servers and pushes updates to parameter servers.
 * <p>
 * It use {@link MasterClient} to communicate with master. The meta data contains: <br>
 * matrix meta {@link MatrixMetaManager}, <br>
 * server locations {@link LocationManager}.
 * 
 * <p>
 * It use {@link MatrixTransportClient} to communicate with parameter servers. Because an
 * application layer request generally needs to access multiple parameter servers, so we use
 * {@link MatrixClientAdapter} first to divide the application layer request to many underlying
 * network requests in matrix partition.
 *
 */
public class PSAgent {
  private static final Log LOG = LogFactory.getLog(PSAgent.class);
  /** application configuration */
  private final Configuration conf;

  /** application id */
  private final ApplicationId appId;

  /** the user that submit the application */
  private final String user;

  /** ps agent attempt id */
  private volatile int id;

  /** the connection manager for rpc to master */
  private final TConnection connection;

  /** the rpc client to master */
  private volatile MasterClient masterClient;

  private volatile PSControlClientManager psControlClientManager;

  /** psagent location(ip and listening port) */
  private volatile Location location;

  /** psagent location(ip and listening port) */
  private final Location masterLocation;

  /** master location(ip and listening port) */
  private volatile PSAgentLocationManager locationManager;

  /** matrix meta manager */
  private volatile PSAgentMatrixMetaManager matrixMetaManager;

  /** matrix updates cache */
  private volatile MatrixOpLogCache opLogCache;

  /** the rpc client to parameter servers */
  private volatile MatrixTransportClient matrixTransClient;

  /** psagent initialization completion flag */
  private final AtomicBoolean psAgentInitFinishedFlag;

  /** psagent heartbeat thread */
  private volatile Thread heartbeatThread;

  /** psagent stop flag */
  private final AtomicBoolean stopped;

  /** psagent heartbeat interval in milliseconds */
  private final int heartbeatIntervalMs;

  /** psagent metrics */
  private final Map<String, String> metrics;

  /** SSP consistency controller */
  private volatile ConsistencyController consistencyController;

  /** matrix partitions clock cache */
  private volatile ClockCache clockCache;

  /** matrices cache */
  private volatile MatricesCache matricesCache;

  /** matrix storage manager */
  private volatile MatrixStorageManager matrixStorageManager;

  /** application layer request adapter */
  private volatile MatrixClientAdapter matrixClientAdapter;

  /** if we need startup heartbeat thread */
  private final boolean needHeartBeat;

  /** application running mode(LOCAL,YARN) */
  private final RunningMode runningMode;

  /** the machine learning executor reference */
  private final Executor executor;

  /** psagent exited flag */
  private final AtomicBoolean exitedFlag;

  /** Control connection manager*/
  private volatile TConnection controlConnectManager;

  /**
   * 
   * Create a new PSAgent instance.
   *
   * @param masterIp master ip
   * @param masterPort master listening port
   * @param clientIndex psagent index
   */
  public PSAgent(String masterIp, int masterPort, int clientIndex) {
    this(new Configuration(), masterIp, masterPort, clientIndex, true, null);
  }

  /**
   * 
   * Create a new PSAgent instance.
   *
   * @param conf application configuration
   * @param masterIp master listening port
   * @param masterPort master listening port
   * @param clientIndex psagent index
   */
  public PSAgent(Configuration conf, String masterIp, int masterPort, int clientIndex) {
    this(conf, masterIp, masterPort, clientIndex, true, null);
  }

  /**
   * 
   * Create a new PSAgent instance.
   *
   * @param conf application configuration
   * @param appId application id
   * @param user the user that submit this application
   * @param masterIp master ip
   * @param masterPort master port
   * @param needHeartBeat true means need startup heartbeat thread
   * @param executor the machine learning executor reference
   */
  public PSAgent(Configuration conf, ApplicationId appId, String user,
      String masterIp, int masterPort, boolean needHeartBeat, Executor executor) {
    this.needHeartBeat = needHeartBeat;
    this.conf = conf;
    this.executor = executor;

    this.heartbeatIntervalMs =
        conf.getInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL,
            AngelConf.DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL);
    this.runningMode = initRunningMode(conf);
    this.appId = appId;
    this.user = user;
    this.masterLocation = new Location(masterIp, masterPort);
    this.connection = TConnectionManager.getConnection(conf);
    this.psAgentInitFinishedFlag = new AtomicBoolean(false);
    this.stopped = new AtomicBoolean(false);
    this.exitedFlag = new AtomicBoolean(false);
    this.metrics = new HashMap<>();
    PSAgentContext.get().setPsAgent(this);
  }

  /**
   * 
   * Create a new PSAgent instance.
   *
   * @param conf application configuration
   * @param masterIp master ip
   * @param masterPort master port
   * @param clientIndex ps agent index
   * @param needHeartBeat true means need startup heartbeat thread
   * @param executor the machine learning executor reference
   */
  public PSAgent(Configuration conf, String masterIp, int masterPort, int clientIndex,
      boolean needHeartBeat, Executor executor) {
    this.needHeartBeat = needHeartBeat;
    this.conf = conf;
    this.executor = executor;
    this.heartbeatIntervalMs =
        conf.getInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL,
            AngelConf.DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL);
    this.runningMode = initRunningMode(conf);

    this.masterLocation = new Location(masterIp, masterPort);
    this.appId = null;
    this.user = null;

    this.connection = TConnectionManager.getConnection(conf);
    this.psAgentInitFinishedFlag = new AtomicBoolean(false);
    this.stopped = new AtomicBoolean(false);
    this.exitedFlag = new AtomicBoolean(false);
    this.metrics = new HashMap<>();
    PSAgentContext.get().setPsAgent(this);
  }

  private RunningMode initRunningMode(Configuration conf) {
    String mode =
        conf.get(AngelConf.ANGEL_RUNNING_MODE,
            AngelConf.DEFAULT_ANGEL_RUNNING_MODE);

    if (mode.equals(RunningMode.ANGEL_PS.toString())) {
      return RunningMode.ANGEL_PS;
    } else {
      return RunningMode.ANGEL_PS_WORKER;
    }
  }

  public void initAndStart() throws Exception {
    // Init control connection manager
    controlConnectManager = TConnectionManager.getConnection(conf);
    
    // Get ps locations from master and put them to the location cache.
    locationManager = new PSAgentLocationManager(PSAgentContext.get());
    locationManager.setMasterLocation(masterLocation);

    // Build and initialize rpc client to master
    masterClient = new MasterClient();
    masterClient.init();

    // Get psagent id
    id = masterClient.getPSAgentId();

    // Build PS control rpc client manager
    psControlClientManager = new PSControlClientManager();

    // Build local location
    String localIp = NetUtils.getRealLocalIP();
    int port = NetUtils.chooseAListenPort(conf);
    location = new Location(localIp, port);
    register();

    // Initialize matrix meta information
    clockCache = new ClockCache();
    List<MatrixMeta> matrixMetas = masterClient.getMatrices();
    LOG.info("PSAgent get matrices from master," + matrixMetas.size());
    this.matrixMetaManager = new PSAgentMatrixMetaManager(clockCache);
    matrixMetaManager.addMatrices(matrixMetas);

    Map<ParameterServerId, Location> psIdToLocMap = masterClient.getPSLocations();
    List<ParameterServerId> psIds = new ArrayList<>(psIdToLocMap.keySet());
    Collections.sort(psIds, new Comparator<ParameterServerId>() {
      @Override public int compare(ParameterServerId s1, ParameterServerId s2) {
        return s1.getIndex() - s2.getIndex();
      }
    });
    int size = psIds.size();
    locationManager.setPsIds(psIds.toArray(new ParameterServerId[0]));
    for(int i = 0; i < size; i++) {
      if(psIdToLocMap.containsKey(psIds.get(i))) {
        locationManager.setPsLocation(psIds.get(i), psIdToLocMap.get(psIds.get(i)));
      }
    }

    matrixTransClient = new MatrixTransportClient();
    matrixClientAdapter = new MatrixClientAdapter();
    opLogCache = new MatrixOpLogCache();

    matrixStorageManager = new MatrixStorageManager();
    matricesCache = new MatricesCache();
    int staleness =
        conf.getInt(AngelConf.ANGEL_STALENESS, AngelConf.DEFAULT_ANGEL_STALENESS);
    consistencyController = new ConsistencyController(staleness);
    consistencyController.init();

    psAgentInitFinishedFlag.set(true);

    // Start all services
    matrixTransClient.start();
    matrixClientAdapter.start();
    clockCache.start();
    opLogCache.start();
    matricesCache.start();   
  }

  /**
   * Get matrix meta from master and refresh the local cache.
   * 
   * @throws ServiceException rpc failed
   * @throws InterruptedException interrupted while wait for rpc results
   */
  public void refreshMatrixInfo()
    throws InterruptedException, ServiceException, ClassNotFoundException {
    matrixMetaManager.addMatrices(masterClient.getMatrices());
  }

  /**
   * Stop ps agent
   */
  public void stop() {
    if (!stopped.getAndSet(true)) {
      LOG.info("stop heartbeat thread!");
      if (heartbeatThread != null) {
        heartbeatThread.interrupt();
        try {
          heartbeatThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping heartbeatThread:", ie);
        }
        heartbeatThread = null;
      }

      LOG.info("stop op log merger");
      if (opLogCache != null) {
        opLogCache.stop();
        opLogCache = null;
      }

      LOG.info("stop clock cache");
      if (clockCache != null) {
        clockCache.stop();
        clockCache = null;
      }

      LOG.info("stop matrix cache");
      if (matricesCache != null) {
        matricesCache.stop();
        matricesCache = null;
      }

      LOG.info("stop matrix client adapater");
      if (matrixClientAdapter != null) {
        matrixClientAdapter.stop();
        matrixClientAdapter = null;
      }

      LOG.info("stop rpc dispacher");
      if (matrixTransClient != null) {
        matrixTransClient.stop();
        matrixTransClient = null;
      }

      if(PSAgentContext.get() != null) {
        PSAgentContext.get().clear();
      }
    }
  }

  protected void heartbeat() throws ServiceException {
    PSAgentReportResponse response = masterClient.psAgentReport();
    switch (response.getCommand()) {
      case PSAGENT_NEED_REGISTER:
        try {
          register();
        } catch (Exception x) {
          LOG.error("register failed: ", x);
          stop();
        }
        break;

      case PSAGENT_SHUTDOWN:
        LOG.error("shutdown command come from appmaster, exit now!!");
        stop();
        break;
      default:
        break;
    }
  }

  private void register() throws ServiceException {
    PSAgentRegisterResponse response = masterClient.psAgentRegister();
    if (response.getCommand() == PSAgentCommandProto.PSAGENT_SHUTDOWN) {
      LOG.fatal("register to master, receive shutdown command");
      stop();
    }
  }

  /**
   * Get application configuration
   * 
   * @return Configuration application configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get master location
   * 
   * @return Location master location
   */
  public Location getMasterLocation() {
    return locationManager.getMasterLocation();
  }

  /**
   * Get application id
   * 
   * @return ApplicationId application id
   */
  public ApplicationId getAppId() {
    return appId;
  }

  /**
   * Get the user that submits the application
   * 
   * @return String the user that submits the application
   */
  public String getUser() {
    return user;
  }

  /**
   * Get the connection manager for rpc to master
   *
   * @return TConnection the connection manager for rpc to master
   */
  public TConnection getConnection() {
    return connection;
  }

  /**
   * get the rpc client to master
   *
   * @return MasterClient the rpc client to master
   */
  public MasterClient getMasterClient() {
    return masterClient;
  }

  /**
   * Get ps agent location ip
   * 
   * @return String ps agent location ip
   */
  public String getIp() {
    return location.getIp();
  }

  /**
   * Notify run success message to master
   */
  public void done() {
    if (!exitedFlag.getAndSet(true)) {
      LOG.info("psagent success done");
      RunningMode mode = PSAgentContext.get().getRunningMode();

      // Stop all modules
      if (executor != null) {
        executor.done();
      } else {
        stop();
      }
    }
  }

  /**
   * Notify run failed message to master
   * 
   * @param errorMsg detail failed message
   */
  public void error(String errorMsg) {
    if (!exitedFlag.getAndSet(true)) {
      LOG.info("psagent falied");
      // Stop all modules
      if (executor != null) {
        executor.error(errorMsg);
      } else {
        stop();
      }
    }
  }

  /**
   * Get ps agent location
   * 
   * @return Location ps agent location
   */
  public Location getLocation() {
    return location;
  }

  /**
   * Get ps location cache
   * 
   * @return LocationCache ps location cache
   */
  public PSAgentLocationManager getLocationManager() {
    return locationManager;
  }

  /**
   * Get matrix meta manager
   * 
   * @return MatrixMetaManager matrix meta manager
   */
  public PSAgentMatrixMetaManager getMatrixMetaManager() {
    return matrixMetaManager;
  }

  /**
   * Get matrix update cache
   * 
   * @return MatrixOpLogCache matrix update cache
   */
  public MatrixOpLogCache getOpLogCache() {
    return opLogCache;
  }

  /**
   * Get rpc client to ps
   * 
   * @return MatrixTransportClient rpc client to ps
   */
  public MatrixTransportClient getMatrixClient() {
    return matrixTransClient;
  }

  /**
   * Get heartbeat interval in milliseconds
   * 
   * @return int heartbeat interval in milliseconds
   */
  public int getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  /**
   * Get ps agent metrics
   * 
   * @return Map<String, String> ps agent metrics
   */
  public Map<String, String> getMetrics() {
    return metrics;
  }

  /**
   * Get SSP consistency controller
   * 
   * @return ConsistencyController SSP consistency controller
   */
  public ConsistencyController getConsistencyController() {
    return consistencyController;
  }

  /**
   * Get matrix clock cache
   * 
   * @return ClockCache matrix clock cache
   */
  public ClockCache getClockCache() {
    return clockCache;
  }

  /**
   * Get matrix cache
   * 
   * @return ClockCache matrix cache
   */
  public MatricesCache getMatricesCache() {
    return matricesCache;
  }

  /**
   * Create a new matrix
   * 
   * @param matrixContext matrix configuration
   * @param timeOutMs maximun wait time in milliseconds
   * @return MatrixMeta matrix meta
   * @throws AngelException exception come from master
   */
  public void createMatrix(MatrixContext matrixContext, long timeOutMs)
      throws AngelException {
    try {
      PSAgentContext.get().getMasterClient().createMatrix(matrixContext, timeOutMs);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  /**
   * Get Matrix meta
   * @param matrixName matrix name
   * @return
   */
  public MatrixMeta getMatrix(String matrixName) {
    try {
      return masterClient.getMatrix(matrixName);
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  private void removeLocalMatrix(String matrixName) {
    int matrixId = matrixMetaManager.getMatrixId(matrixName);
    matrixMetaManager.removeMatrix(matrixId);
    removeCacheData(matrixId);
  }

  private void removeCacheData(int matrixId){
    matricesCache.remove(matrixId);
    matrixStorageManager.removeMatrix(matrixId);
    opLogCache.remove(matrixId);
  }

  /**
   * Release a batch of matrices
   *
   * @param matrixNames matrix names
   * @throws AngelException exception come from master
   */
  public void releaseMatricesUseName(List<String> matrixNames) throws AngelException {
    int size = matrixNames.size();
    for(int i = 0; i < size; i++) {
      removeLocalMatrix(matrixNames.get(i));
    }

    try {
      masterClient.releaseMatrices(matrixNames);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  /**
   * Release a matrix
   * 
   * @param matrixName matrix name
   * @throws AngelException exception come from master
   */
  public void releaseMatrix(String matrixName) throws AngelException {
    try {
      removeLocalMatrix(matrixName);
      masterClient.releaseMatrix(matrixName);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  /**
   * Release a batch of matrices
   *
   * @param matrixIds matrix ids
   * @throws AngelException exception come from master
   */
  public void releaseMatrices(List<Integer> matrixIds) throws AngelException {
    int size = matrixIds.size();
    List<String> matrixNames = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      MatrixMeta meta = matrixMetaManager.getMatrixMeta(matrixIds.get(i));
      if(meta == null) {
        continue;
      }
      matrixNames.add(meta.getName());
    }

    releaseMatricesUseName(matrixNames);
  }

  /**
   * Release a matrix
   *
   * @param matrixId matrix id
   * @throws AngelException exception come from master
   */
  public void releaseMatrix(int matrixId) throws AngelException {
    MatrixMeta meta = matrixMetaManager.getMatrixMeta(matrixId);
    if(meta == null) {
      return;
    }

    releaseMatrix(meta.getName());
  }

  /**
   * Create a batch of matrices
   *
   * @param matrixContexts matrices configuration
   * @param timeOutMs maximun wait time in milliseconds
   * @throws AngelException exception come from master
   */
  public void createMatrices(List<MatrixContext> matrixContexts, long timeOutMs)
    throws AngelException {
    try {
      masterClient.createMatrices(matrixContexts, timeOutMs);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  public List<MatrixMeta> getMatrices(List<String> matrixNames) {
    try {
      return masterClient.getMatrices(matrixNames);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  /**
   * Get matrix rpc client
   * 
   * @param matrixName matrix name
   * @param taskIndex task index
   * @throws AngelException matrix does not exist
   */
  public MatrixClient getMatrixClient(String matrixName, int taskIndex)
      throws AngelException {
    try {
      return MatrixClientFactory.get(matrixName, taskIndex);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  /**
   * Get matrix storage manager
   *
   * @return MatrixStorageManager matrix storage manager
   */
  public MatrixStorageManager getMatrixStorageManager() {
    return matrixStorageManager;
  }

  /**
   * Get application layer request adapter
   * 
   * @return MatrixClientAdapter application layer request adapter
   */
  public MatrixClientAdapter getMatrixClientAdapter() {
    return matrixClientAdapter;
  }

  /**
   * Get application running mode
   *
   * @return RunningMode application running mode
   */
  public RunningMode getRunningMode() {
    return runningMode;
  }

  /**
   * Get the machine learning executor reference
   * 
   * @return Executor the machine learning executor reference
   */
  public Executor getExecutor() {
    return executor;
  }

  /**
   * Get PSAgent ID
   * @return PSAgent ID
   */
  public int getId() {
    return id;
  }

  /**
   * Get control connection manager
   * @return control connection manager
   */
  public TConnection getControlConnectManager() {
    return controlConnectManager;
  }

  /**
   * Get PS control rpc client manager
   * @return PS control rpc client manager
   */
  public PSControlClientManager getPsControlClientManager() {
    return psControlClientManager;
  }
}
