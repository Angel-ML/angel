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

package com.tencent.angel.ps.impl;

import com.google.protobuf.ServiceException;
import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.matrix.transport.MatrixTransportServer;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixStatus;
import com.tencent.angel.protobuf.generated.MLProtos.PSAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Parameter server,hold and manage individual parameters that divided by {@link com.tencent.angel.master.AngelApplicationMaster}.
 * 
 * @see ServerMatrix
 * @see MatrixTransportServer
 *
 */
public class ParameterServer {
  private static final Log LOG = LogFactory.getLog(ParameterServer.class);
  private final Location masterLocation;
  private final Configuration conf;
  private volatile MasterProtocol masterProxy;
  private ParameterServerService psServerService;
  private MatrixTransportServer matrixTransportServer;
  private SnapshotManager snapshotManager;
  //private final ParameterServerId serverId;
  private final PSAttemptId attemptId;
  //private final PSIdProto idProto;
  private final PSAttemptIdProto attemptIdProto;
  private final AtomicBoolean stopped;
  private final int attemptIndex;
  private ParameterServerState state;

  private Thread heartbeatThread;
  private MatrixPartitionManager matrixPartitionManager;

  private MatrixCommitter commiter;

  private static final AtomicInteger runningWorkerGroupNum = new AtomicInteger(0);
  private static final AtomicInteger runningWorekrNum = new AtomicInteger(0);
  private static final AtomicInteger runningTaskNum = new AtomicInteger(0);

  public static int getRunningWorkerGroupNum() {
    return runningWorkerGroupNum.get();
  }

  public static int getRunningWorkerNum() {
    return runningWorekrNum.get();
  }

  public static int getRunningTaskNum() {
    return runningTaskNum.get();
  }

  public static void setRunningWorkerGroupNum(int num) {
    runningWorkerGroupNum.set(num);
  }

  public static void setRunningWorkerNum(int num) {
    runningWorekrNum.set(num);
  }

  public static void setRunningTaskNum(int num) {
    runningTaskNum.set(num);
  }

  /**
   * Create a new Parameter server.
   *
   * @param serverIndex   the server index
   * @param attemptIndex  the attempt index
   * @param appMasterHost the app master host
   * @param appMasterPort the app master port
   * @param conf          the conf
   */
  public ParameterServer(int serverIndex, int attemptIndex, String appMasterHost, int appMasterPort,
      Configuration conf)  {
    this.attemptId = new PSAttemptId(new ParameterServerId(serverIndex), attemptIndex);
    this.attemptIdProto = ProtobufUtil.convertToIdProto(attemptId);
    this.attemptIndex = attemptIndex;
    this.masterLocation = new Location(appMasterHost, appMasterPort);
    this.conf = conf;
    this.stopped = new AtomicBoolean(false);
  }

  /**
   * Gets matrix partition manager.
   *
   * @return the matrix partition manager
   */
  public MatrixPartitionManager getMatrixPartitionManager() {
    return matrixPartitionManager;
  }

  public SnapshotManager getSnapshotManager() {
    return snapshotManager;
  }

  /**
   * Stop parameter server.
   *
   * @param exitCode the exit code
   */
  public void stop(int exitCode) {
    LOG.info("stop ps rpcServer!");
    if (psServerService != null) {
      psServerService.stop();
    }
    LOG.info("stop heartbeat thread!");
    if (!stopped.getAndSet(true)) {
      if (heartbeatThread != null) {
        heartbeatThread.interrupt();
        try {
          heartbeatThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping heartbeatThread.");
        }
        heartbeatThread = null;
      }
      
      if(matrixTransportServer != null) {
        try {
          matrixTransportServer.stop();         
        } catch (InterruptedException e) {
          LOG.warn("stop matrixTransportServer interrupted.");
        }
        matrixTransportServer = null;
      }
      
      if(snapshotManager != null) {
        snapshotManager.stop();
      }
      exit(exitCode);
    }
  }

  private  void exit(int code) {
    AngelDeployMode deployMode = PSContext.get().getDeployMode();
    if(deployMode == AngelDeployMode.YARN) {
      System.exit(code);
    }
  }

  public static void main(String[] argv)  {
    LOG.info("Starting Parameter Server");
    int serverIndex = Integer.valueOf(System.getenv(AngelEnvironment.PARAMETERSERVER_ID.name()));
    String appMasterHost = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    int appMasterPort = Integer.valueOf(System.getenv(AngelEnvironment.LISTEN_PORT.name()));

    int attemptIndex = Integer.valueOf(System.getenv(AngelEnvironment.PS_ATTEMPT_ID.name()));

    Configuration conf = new Configuration();
    conf.addResource(AngelConfiguration.ANGEL_JOB_CONF_FILE);

    String user = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation.setConfiguration(conf);
    
    String runningMode = conf.get(AngelConfiguration.ANGEL_RUNNING_MODE, 
        AngelConfiguration.DEFAULT_ANGEL_RUNNING_MODE);   
    if(runningMode.equals(RunningMode.ANGEL_PS_WORKER.toString())){
      LOG.debug("AngelEnvironment.TASK_NUMBER.name()=" + AngelEnvironment.TASK_NUMBER.name());
      conf.set(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM,
          System.getenv(AngelEnvironment.TASK_NUMBER.name()));
    }

    final ParameterServer psServer =
        new ParameterServer(serverIndex, attemptIndex, appMasterHost, appMasterPort, conf);

    PSContext.get().setPs(psServer);

    try{
      Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
      UserGroupInformation psUGI = UserGroupInformation.createRemoteUser(System
        .getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      psUGI.addCredentials(credentials);

      psUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          psServer.initialize();
          psServer.start();
          return null;
        }
      });
    } catch (Throwable x) {
      LOG.fatal("Start PS failed ", x);
      psServer.failed(x.getMessage());
    }
    LOG.info("Starting Parameter Server successfully.");
  }

  /**
   * Gets host address.
   *
   * @return the host address
   * @throws UnknownHostException
   */
  public String getHostAddress() throws UnknownHostException {
    return psServerService.getHostAddress();
  }

  /**
   * Gets port.
   *
   * @return the port
   */
  public int getPort() {
    return psServerService.getPort();
  }

  /**
   * Gets server id.
   *
   * @return the server id
   */
  public ParameterServerId getServerId() {
    return attemptId.getParameterServerId();
  }

  /**
   * Gets ps attempt id.
   *
   * @return the ps attempt id
   */
  public PSAttemptId getPSAttemptId() {
    return attemptId;
  }

  /**
   * Gets master location.
   *
   * @return the master location
   */
  public Location getMasterLocation() {
    return masterLocation;
  }

  /**
   * Gets conf.
   *
   * @return the conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Initialize.
   *
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public void initialize() throws IOException, InstantiationException, IllegalAccessException {
    LOG.info("Initialize a parameter server");

    matrixPartitionManager = new MatrixPartitionManager();
    commiter = new MatrixCommitter(this);
    TConnection connection = TConnectionManager.getConnection(conf);
    try {
      masterProxy = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
    } catch (IOException e) {
      LOG.error("Connect to master failed! PS is to exit now!", e);
      stop(-1);
    }
    
    psServerService = new ParameterServerService();
    psServerService.start();
    matrixTransportServer = new MatrixTransportServer(getPort() + 1); 
    snapshotManager = new SnapshotManager(attemptId);
    snapshotManager.init();
  }

  private void startHeartbeart() {
    final int heartbeatInterval =
        conf.getInt(AngelConfiguration.ANGEL_PS_HEARTBEAT_INTERVAL_MS,
            AngelConfiguration.DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS);
    LOG.info("Starting HeartbeatThread, interval is " + heartbeatInterval + " ms");
    heartbeatThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(heartbeatInterval);
            try {
              heartbeat();
            } catch (Exception netException) {
              LOG.error("Error communicating with AM: ", netException);
              return;
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Heartbeat thread interrupted. Returning.");
            }
          }
        }
      }
    });
    heartbeatThread.setName("heartbeatThread");
    heartbeatThread.start();
  }

  private void register() throws IOException {
    LOG.info("Registing to AppMaster " + masterLocation);
    PSRegisterRequest.Builder regBuilder = PSRegisterRequest.newBuilder();
    regBuilder.setPsAttemptId(attemptIdProto);
    try {    
      Location location = new Location(InetAddress.getLocalHost().getHostAddress(), psServerService.getPort());
      regBuilder.setLocation(ProtobufUtil.convertLocation(location));
    } catch (UnknownHostException eop) {
      LOG.error("UnknownHostException: " + eop);
      throw new IOException(eop);
    }
    
    try {
      masterProxy.psRegister(null, regBuilder.build());
      LOG.info("Register to AppMaster successfully");
    } catch (ServiceException e) {
      // to exit
      LOG.error("ps register to appmaster failed: ", e);
      stop(-1);
    }
  }

  private List<MatrixReport> buildMatrixReports() {
    MatrixReport.Builder builder = MatrixReport.newBuilder();
    List<MatrixReport> ret = new ArrayList<MatrixReport>();
    ConcurrentHashMap<Integer, ServerMatrix> matrixIdMap = matrixPartitionManager.getMatrixIdMap();
    for (Entry<Integer, ServerMatrix> matrixEntry : matrixIdMap.entrySet()) {
      ret.add(builder.setMatrixId(matrixEntry.getKey())
          .setMatrixName(matrixEntry.getValue().getName()).setStatus(MatrixStatus.M_OK).build());
    }
    return ret;
  }

  private void heartbeat() {
    PSReportRequest.Builder builder = PSReportRequest.newBuilder();
    builder.setPsAttemptId(attemptIdProto);
    Pair.Builder pairBuilder = Pair.newBuilder();
    pairBuilder.setKey("key");
    pairBuilder.setValue("value");
    builder.addMetrics(pairBuilder.build());
    builder.addAllMatrixReports(buildMatrixReports());

    PSReportResponse ret = null;
    try {
      ret = masterProxy.psReport(null, builder.build());
      LOG.debug("heartbeat response " + ret);
      switch (ret.getPsCommand()) {
        case PSCOMMAND_REGISTER:
          try {
            register();
          } catch (Exception x) {
            LOG.error("register failed: ", x);
            stop(-1);
          }
          break;
          
        case PSCOMMAND_SHUTDOWN:
          LOG.error("shutdown command come from appmaster, exit now!!");
          stop(-1);
          break;
          
        case PSCOMMAND_COMMIT:
          LOG.info("received ps commit command, ps is committing now!");
          LOG.info("to stop taskSnapshotsThread.");
          snapshotManager.stop();
          commiter.commit(ret.getNeedCommitMatrixIdsList());
          break;
          
        default:
          break;
      }

      syncMatrixInfo(ret.getNeedCreateMatrixIdsList(), ret.getNeedReleaseMatrixIdsList());
    } catch (ServiceException e) {
      LOG.error("send heartbeat to appmaster failed ", e);
      stop(-1);
    }
  }

  private void syncMatrixInfo(List<MatrixPartition> needCreateMatrixesList,
      List<Integer> needReleaseMatrixesList) {
    try {
      matrixPartitionManager.addMatrixPartitions(needCreateMatrixesList);
    } catch (IOException e) {
      LOG.fatal("init matrix failed, exit now ", e);
      stop(-1);
    }
    PSContext.get().getSnapshotManager().processRecovery();
    matrixPartitionManager.removeMatrices(needReleaseMatrixesList);
  }

  /**
   * Start parameter server services.
   *
   * @throws IOException the io exception
   */
  public void start() throws IOException { 
    matrixTransportServer.start();

    register();
    startHeartbeart();
    snapshotManager.start();
  }

  /**
   * Done, will notify master and exit
   */
  public void done() {
    try {
      masterProxy.psDone(null, PSDoneRequest.newBuilder().setPsAttemptId(attemptIdProto).build());
      LOG.info("send done message to master success");
    } catch (ServiceException e) {
      LOG.error("send done message to master failed ", e);
    } finally {
      stop(0);
    }
  }

  /**
   * Failed, will notify master and exit
   *
   * @param errorLog the error log
   */
  public void failed(String errorLog) {
    try {
      masterProxy.psError(null, PSErrorRequest.newBuilder().setPsAttemptId(attemptIdProto).setMsg(errorLog).build());
      LOG.info("send failed message to master success");
    } catch (ServiceException e) {
      LOG.error("send failed message to master failed ", e);
    } finally {
      stop(-1);
    }
  }

  /**
   * Gets state.
   *
   * @return the state
   */
  public ParameterServerState getState() {
    return state;
  }

  /**
   * Gets parameter server service.
   *
   * @return the ps server service
   */
  public ParameterServerService getPsService() {
    return psServerService;
  }

  /**
   * Gets rpc client to master
   * @return MasterProtocol rpc client to master
   */
  public MasterProtocol getMaster() {
    return masterProxy;
  }

  public int getAttemptIndex() {
    return attemptIndex;
  }
}
