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
import com.tencent.angel.PartitionKey;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.exception.TimeOutException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.consistency.ConsistencyController;
import com.tencent.angel.psagent.executor.Executor;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.MatrixClientFactory;
import com.tencent.angel.psagent.matrix.cache.MatricesCache;
import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLogCache;
import com.tencent.angel.psagent.matrix.storage.MatrixStorageManager;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.adapter.MatrixClientAdapter;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.PSAgentAttemptIdProto;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.GetAllMatrixInfoResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentCommandProto;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentRegisterResponse;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.PSAgentReportResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Agent for master and parameter servers, It fetches matrix meta from master, fetches matrices from
 * parameter servers and pushes updates to parameter servers.
 * <p>
 * It use {@link MasterClient} to communicate with master. The meta data contains: <br>
 * matrix meta {@link MatrixMetaManager}, <br>
 * matrix partition routers {@link MatrixPartitionRouter}, <br>
 * server locations {@link LocationCache}.
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

  /** master location(ip and listening port) */
  private final Location masterLocation;

  /** ps agent attempt id */
  private final PSAgentAttemptId id;

  /** ps agent attempt id for rpc */
  private final PSAgentAttemptIdProto idProto;

  /** the connection manager for rpc to master */
  private final TConnection connection;

  /** the rpc client to master */
  private MasterClient masterClient = null;

  /** psagent location(ip and listening port) */
  private Location location = null;

  /** parameter server locations cache */
  private LocationCache locationCache = null;

  /** matrix partitions router table */
  private MatrixPartitionRouter matrixPartitionRouter = null;

  /** matrix meta manager */
  private MatrixMetaManager matrixMetaManager = null;

  /** matrix updates cache */
  private MatrixOpLogCache opLogCache = null;

  /** the rpc client to parameter servers */
  private MatrixTransportClient matrixTransClient = null;

  /** psagent initialization completion flag */
  private final AtomicBoolean psAgentInitFinishedFlag;

  /** psagent heartbeat thread */
  private Thread heartbeatThread = null;

  /** psagent stop flag */
  private final AtomicBoolean stopped;

  /** psagent heartbeat interval in milliseconds */
  private final int heartbeatIntervalMs;

  /** psagent metrics */
  private final Map<String, String> metrics;

  /** SSP consistency controller */
  private ConsistencyController consistencyController;

  /** matrix partitions clock cache */
  private ClockCache clockCache;

  /** matrices cache */
  private MatricesCache matricesCache;

  /** matrix storage manager */
  private MatrixStorageManager matrixStorageManager;

  /** application layer request adapter */
  private MatrixClientAdapter matrixClientAdapter;

  /** if we need startup heartbeat thread */
  private final boolean needHeartBeat;

  /** application running mode(LOCAL,YARN) */
  private final RunningMode runningMode;

  /** the machine learning executor reference */
  private final Executor executor;

  /** psagent exited flag */
  private final AtomicBoolean exitedFlag;

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
   * @param id ps agent id
   * @param masterIp master ip
   * @param masterPort master port
   * @param needHeartBeat true means need startup heartbeat thread
   * @param executor the machine learning executor reference
   */
  public PSAgent(Configuration conf, ApplicationId appId, String user, PSAgentAttemptId id,
      String masterIp, int masterPort, boolean needHeartBeat, Executor executor) {
    this.needHeartBeat = needHeartBeat;
    this.conf = conf;
    this.executor = executor;

    this.heartbeatIntervalMs =
        conf.getInt(AngelConfiguration.ANGEL_WORKER_HEARTBEAT_INTERVAL,
            AngelConfiguration.DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL);
    this.runningMode = initRunningMode(conf);
    this.appId = appId;
    this.user = user;
    this.masterLocation = new Location(masterIp, masterPort);
    this.id = id;
    this.idProto = ProtobufUtil.convertToIdProto(id);
    this.connection = TConnectionManager.getConnection(conf);
    this.psAgentInitFinishedFlag = new AtomicBoolean(false);
    this.stopped = new AtomicBoolean(false);
    this.exitedFlag = new AtomicBoolean(false);
    this.metrics = new HashMap<String, String>();
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
        conf.getInt(AngelConfiguration.ANGEL_WORKER_HEARTBEAT_INTERVAL,
            AngelConfiguration.DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL);
    this.runningMode = initRunningMode(conf);

    this.appId = null;
    this.user = null;
    this.masterLocation = new Location(masterIp, masterPort);
    this.id = new PSAgentAttemptId(new PSAgentId(clientIndex), 0);
    this.idProto = ProtobufUtil.convertToIdProto(id);

    this.connection = TConnectionManager.getConnection(conf);
    this.psAgentInitFinishedFlag = new AtomicBoolean(false);
    this.stopped = new AtomicBoolean(false);
    this.exitedFlag = new AtomicBoolean(false);
    this.metrics = new HashMap<String, String>();
    PSAgentContext.get().setPsAgent(this);
  }

  private RunningMode initRunningMode(Configuration conf) {
    String mode =
        conf.get(AngelConfiguration.ANGEL_RUNNING_MODE,
            AngelConfiguration.DEFAULT_ANGEL_RUNNING_MODE);

    if (mode.equals(RunningMode.ANGEL_PS.toString())) {
      return RunningMode.ANGEL_PS;
    } else if (mode.equals(RunningMode.ANGEL_PS_PSAGENT.toString())) {
      return RunningMode.ANGEL_PS_PSAGENT;
    } else {
      return RunningMode.ANGEL_PS_WORKER;
    }
  }

  public void initAndStart() throws Exception {
    // Build and initialize rpc client to master
    masterClient = new MasterClient();
    masterClient.init();

    // Build local location
    String localIp = NetUtils.getRealLocalIP();
    int port = NetUtils.chooseAListenPort(conf);
    location = new Location(localIp, port);

    // Initialize matrix info, this method will wait until master accepts the information from
    // client
    GetAllMatrixInfoResponse response = masterClient.getMatrices();
    LOG.debug("get matrix info=" + response);

    // Get ps locations from master and put them to the location cache.
    locationCache = new LocationCache(masterLocation, masterClient.getPSLocations());
    this.matrixPartitionRouter =
        new MatrixPartitionRouter(ProtobufUtil.getMatrixRoutingInfo(response));

    // Initialize matrix meta information
    this.matrixMetaManager = new MatrixMetaManager();
    matrixMetaManager.addMatrixes(ProtobufUtil.getMatrixMetaInfo(response));

    matrixTransClient = new MatrixTransportClient();
    matrixClientAdapter = new MatrixClientAdapter();
    opLogCache = new MatrixOpLogCache();
    clockCache = new ClockCache();
    Map<Integer, List<PartitionKey>> matrixToPartMap = matrixPartitionRouter.getMatrixToPartMap();
    for (Entry<Integer, List<PartitionKey>> entry : matrixToPartMap.entrySet()) {
      clockCache.addMatrix(entry.getKey(), entry.getValue());
    }

    matrixStorageManager = new MatrixStorageManager();

    matricesCache = new MatricesCache();
    int staleness =
        conf.getInt(AngelConfiguration.ANGEL_STALENESS, AngelConfiguration.DEFAULT_ANGEL_STALENESS);
    consistencyController = new ConsistencyController(staleness);
    consistencyController.init();

    psAgentInitFinishedFlag.set(true);

    // Start heartbeat thread if need
    if (needHeartBeat) {
      startHeartbeatThread();
    }

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
  public void refreshMatrixInfo() throws InterruptedException, ServiceException  {
    GetAllMatrixInfoResponse response = masterClient.getMatrices();
    HashMap<PartitionKey, ParameterServerId> routingInfo =
        ProtobufUtil.getMatrixRoutingInfo(response);
    matrixPartitionRouter.clear();
    for (Entry<PartitionKey, ParameterServerId> entry : routingInfo.entrySet()) {
      PartitionKey part = entry.getKey();
      ParameterServerId psId = entry.getValue();
      matrixPartitionRouter.addPartition(entry.getKey(), psId);
      Map<Integer, List<PartitionKey>> matrixToParts = matrixPartitionRouter.getMatrixToPartMap();
      List<PartitionKey> partList = matrixToParts.get(part.getMatrixId());
      if (partList == null) {
        partList = new ArrayList<PartitionKey>();
        matrixToParts.put(part.getMatrixId(), partList);
      }
      partList.add(part);
    }
    matrixMetaManager.addMatrixes(ProtobufUtil.getMatrixMetaInfo(response));
  }

  private void startHeartbeatThread() {
    heartbeatThread = new Thread(new Runnable() {
      @Override
      public void run() {
        // Register to master first
        try {
          register();
        } catch (Exception x) {
          LOG.error("register am to rm error:" + x);
          stop();
          if (runningMode == RunningMode.ANGEL_PS_PSAGENT) {
            System.exit(-1);
          }
        }

        // Report state to master every specified time
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(heartbeatIntervalMs);
            try {
              heartbeat();
            } catch (YarnRuntimeException e) {
              LOG.error("Error communicating with RM: " + e.getMessage(), e);
              stop();
              if (runningMode == RunningMode.ANGEL_PS_PSAGENT) {
                System.exit(-1);
              }
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              continue;
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Allocated thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    heartbeatThread.setName("heartbeat");
    heartbeatThread.start();
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
          if (runningMode == RunningMode.ANGEL_PS_PSAGENT) {
            System.exit(-1);
          }
        }
        break;

      case PSAGENT_SHUTDOWN:
        LOG.error("shutdown command come from appmaster, exit now!!");
        stop();
        if (runningMode == RunningMode.ANGEL_PS_PSAGENT) {
          System.exit(-1);
        }
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
      if (runningMode == RunningMode.ANGEL_PS_PSAGENT) {
        System.exit(-1);
      }
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
    return masterLocation;
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
   * Get ps agent attempt id
   * 
   * @return PSAgentAttemptId ps agent attempt id
   */
  public PSAgentAttemptId getId() {
    return id;
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

      // Notify run success to master only on ANGEL_PS_PSAGENT running mode
      if (mode == RunningMode.ANGEL_PS_PSAGENT) {
        try {
          masterClient.psAgentDone();
          LOG.info("send done message to appmaster success");
        } catch (ServiceException e) {
          LOG.error("send done message error ", e);
        } finally {
          try {
            connection.close();
          } catch (Exception e) {
            LOG.error("close connection error", e);
          }
        }
      }

      // Stop all modules
      if (executor != null) {
        executor.done();
      } else {
        stop();
      }

      // Exit the process if on ANGEL_PS_PSAGENT mode
      if (mode == RunningMode.ANGEL_PS_PSAGENT) {
        System.exit(0);
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

      // Notify run success to master only on ANGEL_PS_PSAGENT running mode
      RunningMode mode = PSAgentContext.get().getRunningMode();
      if (mode == RunningMode.ANGEL_PS_PSAGENT) {
        try {
          masterClient.psAgentError(errorMsg);
          LOG.info("psagent failed message : " + errorMsg + ", send it to appmaster success");
        } catch (ServiceException e) {
          LOG.error("send error message error ", e);
        } finally {
          try {
            connection.close();
          } catch (Exception e) {
            LOG.error("close connection error", e);
          }
        }
      }

      // Stop all modules
      if (executor != null) {
        executor.error(errorMsg);
      } else {
        stop();
      }

      // Exit the process if on ANGEL_PS_PSAGENT mode
      if (mode == RunningMode.ANGEL_PS_PSAGENT) {
        System.exit(-1);
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
  public LocationCache getLocationCache() {
    return locationCache;
  }

  /**
   * Get matrix partition router manager
   * 
   * @return MatrixPartitionRouter matrix partition router manager
   */
  public MatrixPartitionRouter getMatrixPartitionRouter() {
    return matrixPartitionRouter;
  }

  /**
   * Get matrix meta manager
   * 
   * @return MatrixMetaManager matrix meta manager
   */
  public MatrixMetaManager getMatrixMetaManager() {
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

  public static void main(String[] args) {
    // get configuration from config file
    Configuration conf = new Configuration();
    conf.addResource(AngelConfiguration.ANGEL_JOB_CONF_FILE);

    String containerIdStr = System.getenv(Environment.CONTAINER_ID.name());
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();
    String user = System.getenv(Environment.USER.name());

    // set localDir with enviroment set by nm.
    String[] localSysDirs =
        StringUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name()));
    conf.setStrings(AngelConfiguration.LOCAL_DIR, localSysDirs);
    LOG.info(AngelConfiguration.LOCAL_DIR + " for child: " + conf.get(AngelConfiguration.LOCAL_DIR));

    String psAgentindex = System.getenv(AngelEnvironment.PSAGENT_ID.name());
    String psAgentAttemptIndex = System.getenv(AngelEnvironment.PSAGENT_ATTEMPT_ID.name());
    String masterAddr = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    String portStr = System.getenv(AngelEnvironment.LISTEN_PORT.name());
    Location masterLocation = new Location(masterAddr, Integer.valueOf(portStr));

    LOG.info("psAgentindex=" + psAgentindex);
    LOG.info("psAgentAttemptIndex=" + psAgentAttemptIndex);
    LOG.info("masterLocation=" + masterLocation);
    LOG.info("user=" + user);
    LOG.info("appId=" + appId);

    PSAgentId psAgentId = new PSAgentId(Integer.valueOf(psAgentindex));
    PSAgentAttemptId psAgentAttemptId =
        new PSAgentAttemptId(psAgentId, Integer.valueOf(psAgentAttemptIndex));

    try {
      PSAgent psAgent =
          new PSAgent(conf, appId, user, psAgentAttemptId, masterAddr, Integer.valueOf(portStr),
              true, null);
      psAgent.initAndStart();
    } catch (Exception e) {
      LOG.fatal("Failed to start worker.", e);
    }
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
   * Get ps agent attempt id used for rpc
   * 
   * @return PSAgentAttemptIdProto ps agent attempt id used for rpc
   */
  public PSAgentAttemptIdProto getIdProto() {
    return idProto;
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
   * @throws ServiceException exception come from master
   * @throws TimeOutException create matrix time out
   * @throws InterruptedException interrupted when wait
   * @throws IOException read matrix meta from hdfs failed
   */
  public MatrixMeta createMatrix(MatrixContext matrixContext, long timeOutMs)
      throws ServiceException, TimeOutException, InterruptedException, IOException {
    MatrixMeta matrix =
        PSAgentContext.get().getMasterClient().createMatrix(matrixContext, timeOutMs);
    return matrix;
  }

  /**
   * Release a matrix
   * 
   * @param matrix matrix meta
   * @throws ServiceException exception come from master
   * @throws InterruptedException interrupted when wait
   */
  public void releaseMatrix(MatrixMeta matrix) throws ServiceException, InterruptedException {
    PSAgentContext.get().getMasterClient().releaseMatrix(matrix);
  }

  /**
   * Get matrix rpc client
   * 
   * @param matrixName matrix name
   * @param taskIndex task index
   * @throws InvalidParameterException matrix does not exist
   * @return MatrixClient matrix client
   */
  public MatrixClient getMatrixClient(String matrixName, int taskIndex)
      throws InvalidParameterException {
    return MatrixClientFactory.get(matrixName, taskIndex);
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
}
