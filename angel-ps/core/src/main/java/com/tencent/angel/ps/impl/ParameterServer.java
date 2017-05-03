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
import com.tencent.angel.exception.InitMatrixException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.matrix.transport.MatrixTransportServer;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixStatus;
import com.tencent.angel.protobuf.generated.MLProtos.PSAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
  private MasterProtocol masterProxy;
  private ParameterServerService psServerService;
  //private final ParameterServerId serverId;
  private final PSAttemptId attemptId;
  //private final PSIdProto idProto;
  private final PSAttemptIdProto attemptIdProto;
  private FileSystem fs;
  private FileContext fileContext;
  private AtomicBoolean stopped;
  private int attemptIndex;
  private ParameterServerState state;

  private Thread heartbeatThread;
  private Thread taskSnapshotsThread;
  private MatrixPartitionManager matrixPartitionManager;
  private Path snapShotBasePath;
  private MatrixCommitter commiter;

  private static String snapshots = "snapshots";
  private static String tmp = "_temporary";
  private static String tempSnapshotFileName = "snapshots_";
  private static String snapshotFileName = "snapshots";
  private static int snapshotFileIndex = 0;

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

  private Path createSnapshotBaseDir(FileSystem fs, String outputPath) throws IOException {
    String baseDirStr =
        outputPath + Path.SEPARATOR + snapshots + Path.SEPARATOR
            + String.valueOf(attemptId.getParameterServerId().getIndex()) + Path.SEPARATOR + String.valueOf(attemptIndex);

    Path basePath = new Path(baseDirStr);
    LOG.info("create snapshot base directory:" + baseDirStr);

    if (!fs.exists(basePath)) {
      fs.mkdirs(basePath);
    }
    return basePath;
  }

  /**
   * Write snapshots of matrices for recovery.
   *
   * @throws IOException the io exception
   */
  public void writeSnapshots() throws IOException {
    LOG.info("start to write matrix snapshot");
    long startTime = Time.monotonicNow();
    Path snapshotsTempDirPath = getPSSnapshotsTempDir();
    Path snapshotsTempFilePath = new Path(snapshotsTempDirPath, tempSnapshotFileName);
    // FSDataOutputStream output = fileContext.create(snapshotsTempFilePath,
    // EnumSet.of(CreateFlag.CREATE));
    FSDataOutputStream output = fs.create(snapshotsTempFilePath);
    LOG.info("write matrix snapshot to " + snapshotsTempFilePath);
    matrixPartitionManager.writeMatrix(output);
    output.flush();
    output.close();
    LOG.info("write matrix snapshot over");

    Path snapshotsDestFilePath = getPSSnapshotDestFile();
    fs.rename(snapshotsTempFilePath, snapshotsDestFilePath);
    LOG.info("rename " + snapshotsTempFilePath + " to " + snapshotsDestFilePath + " success");
    Path oldSnapshotFile = getOldSnapshotDestFile();
    if (oldSnapshotFile != null) {
      LOG.info("deleting old snapshotFile: " + oldSnapshotFile);
      fs.delete(oldSnapshotFile, false);
    }
    LOG.info("write snapshots cost " + (Time.monotonicNow() - startTime) + "ms!");
  }


  /*
   * @brief get next filename for snapshot
   */
  private Path getPSSnapshotDestFile() throws IOException {
    return new Path(snapShotBasePath, snapshotFileName + "_" + (snapshotFileIndex++));
  }

  // @brief get filename of the old snapshot written before
  private Path getOldSnapshotDestFile() {
    if (snapshotFileIndex <= 1) {
      // no snapshotFile write before, maybe write snapshots the first time
      return null;
    }
    return new Path(snapShotBasePath, snapshotFileName + "_" + (snapshotFileIndex - 2));
  }

  private Path getPSSnapshotsTempDir() throws IOException {
    Path tempSnapshotDir = new Path(snapShotBasePath, tmp);
    if (!fs.exists(tempSnapshotDir)) {
      fs.mkdirs(tempSnapshotDir);
    }
    return tempSnapshotDir;
  }

  private Path getLastSnapshotsFile(Path lastAttemptSnapshotPath) throws IOException {
    Path snapshotsFile = null;
    FileStatus[] allFileStatus = fs.listStatus(lastAttemptSnapshotPath);
    for (FileStatus fileStatus : allFileStatus) {
      if (fileStatus.isFile()) {
        if (snapshotsFile == null) {
          snapshotsFile = fileStatus.getPath();
        } else {
          if (fileStatus.getPath().getName().compareTo(snapshotsFile.getName()) > 0) {
            LOG.info("old snapshotsFile is: " + snapshotsFile + ", new snapshotsFile is: "
                + fileStatus.getPath());
            snapshotsFile = fileStatus.getPath();
          }
        }
      }
    }
    return snapshotsFile;
  }

  private Path getPreviousPSSnapshotsPath() throws IOException {
    Path lastAttemptSnapshotPath = null;
    Path lastAttemptSnapshotDir = null;
    int lastAttempt = attemptIndex;
    while (lastAttempt >= 0) {
      lastAttemptSnapshotDir = new Path(snapShotBasePath.getParent(), String.valueOf(lastAttempt));
      if (fs.exists(lastAttemptSnapshotDir)) {
        lastAttemptSnapshotPath = getLastSnapshotsFile(lastAttemptSnapshotDir);
        if (lastAttemptSnapshotPath == null) {
          lastAttempt--;
          LOG.warn("no snapshotFile in " + lastAttemptSnapshotDir);
          continue;
        }
        break;
      } else {
        LOG.warn("ps: " + attemptId.getParameterServerId() + ", attempt " + lastAttempt
            + " failed without write snapshots!");
        lastAttemptSnapshotPath = null;
        lastAttempt--;
      }
    }
    return lastAttemptSnapshotPath;
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
      if (taskSnapshotsThread != null) {
        taskSnapshotsThread.interrupt();
        try {
          taskSnapshotsThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping taskSnapshotsThread.");
        }
      }
      taskSnapshotsThread = null;
      exit(exitCode);
    }
  }

  private  void exit(int code) {
    AngelDeployMode deployMode = PSContext.get().getDeployMode();
    if(deployMode == AngelDeployMode.YARN) {
      System.exit(code);
    }
  }


  public static void main(String[] argv) throws IOException, InstantiationException,
      IllegalAccessException {
    LOG.info("Starting Parameter Server");
    int serverIndex = Integer.valueOf(System.getenv(AngelEnvironment.PARAMETERSERVER_ID.name()));
    String appMasterHost = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    int appMasterPort = Integer.valueOf(System.getenv(AngelEnvironment.LISTEN_PORT.name()));

    int attemptIndex = Integer.valueOf(System.getenv(AngelEnvironment.PS_ATTEMPT_ID.name()));

    Configuration conf = new Configuration();
    conf.addResource(AngelConfiguration.ANGEL_JOB_CONF_FILE);
    
    String runningMode = conf.get(AngelConfiguration.ANGEL_RUNNING_MODE, 
        AngelConfiguration.DEFAULT_ANGEL_RUNNING_MODE);   
    if(runningMode.equals(RunningMode.ANGEL_PS_WORKER.toString())){
      LOG.debug("AngelEnvironment.TASK_NUMBER.name()=" + AngelEnvironment.TASK_NUMBER.name());
      conf.set(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM,
          System.getenv(AngelEnvironment.TASK_NUMBER.name()));
    }

    ParameterServer psServer =
        new ParameterServer(serverIndex, attemptIndex, appMasterHost, appMasterPort, conf);

    PSContext.get().setPs(psServer);

    psServer.initialize();
    psServer.start();
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

    String outputPath = conf.get(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY);
    LOG.info("tmp output dir=" + outputPath);
    if (outputPath == null) {
      throw new IOException("can not find output path setting");
    }

    fs = new Path(outputPath).getFileSystem(conf);
    snapShotBasePath = createSnapshotBaseDir(fs, outputPath);

    fileContext = FileContext.getFileContext(conf);
    TConnection connection = TConnectionManager.getConnection(conf);
    try {
      masterProxy = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
    } catch (IOException e) {
      LOG.error("Connect to master failed! PS is to exit now!", e);
      stop(-1);
    }
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

  private void startTakeSnapshotsThread() {
    // TODO
    // when we should write snapshot to hdfs? clearly, we have two methods:
    // 1. write snapshot at regular time, if there are updates, just write them.
    // 2. write snapshot every N iterations, this method depends on notification of master
    final int backupInterval =
        conf.getInt(AngelConfiguration.ANGEL_PS_BACKUP_INTERVAL_MS,
            AngelConfiguration.DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS);
    LOG.info("Starting TakeSnapshotsThread, backup interval is " + backupInterval + " ms");
    taskSnapshotsThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("TakeSnapshotsThread is to sleep");
            }
            Thread.sleep(backupInterval);
            try {
              LOG.info("to writeSnapshots");
              writeSnapshots();
            } catch (Exception ioe) {
              LOG.error("Taking snapshots error: ", ioe);
              return;
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("TakeSnapShotsThread interrupted. Returning.");
            }
          }
        }
      }
    });
    taskSnapshotsThread.setName("taskSnapshotsThread");
    taskSnapshotsThread.start();
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
          taskSnapshotsThread.interrupt();
          commiter.commit(ret.getNeedCommitMatrixIdsList());
          break;
        default:
          break;
      }

      syncMatrixInfo(ret.getNeedCreateMatrixIdsList(), ret.getNeedReleaseMatrixIdsList());

      // LOG.debug("heartbeat execute unit desc = " + ret.getExecuteUnitDesc());
      // setRunningWorkerGroupNum(ret.getExecuteUnitDesc().getActiveWorkerGroupNum());
      // setRunningWorkerNum(ret.getExecuteUnitDesc().getActiveWorkerNum());
      // setRunningTaskNum(ret.getExecuteUnitDesc().getActiveTaskNum());

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

    matrixPartitionManager.removeMatrices(needReleaseMatrixesList);
  }

  /**
   * Start parameter server services.
   *
   * @throws IOException the io exception
   */
  public void start() throws IOException {
    // get matrix partitions from master before register, ensure all services in parameterserver
    // are initialized before worker access, we should use matrix partition file next step
    List<MatrixPartition> matrixPartitions = getMatrixInfo();
    
    // initialize matrix    
    matrixPartitionManager.addMatrixPartitions(matrixPartitions);

    // recover parameter matrix from last attempt if needed
    processRecovery();
    GetTaskMatrixClockResponse taskMatrixClocks = null;
    try {
       taskMatrixClocks = getTaskMatrixClocks();
       LOG.debug("taskMatrixClocks=" + taskMatrixClocks);
       adjustMatrixClocks(taskMatrixClocks);
    } catch (ServiceException e) {
      LOG.error("get task clocks from master failed.", e);
    }
    psServerService = new ParameterServerService(this);
    psServerService.start();

    MatrixTransportServer matrixTransportServer = new MatrixTransportServer(getPort() + 1);
    matrixTransportServer.start();

    // getExecuteUnitNum();

    register();
    startHeartbeart();
    startTakeSnapshotsThread();
  }

  private List<MatrixPartition> getMatrixInfo() throws IOException {
    GetMatrixPartitionRequest.Builder builder = GetMatrixPartitionRequest.newBuilder();
    builder.setPsAttemptId(attemptIdProto);
    GetMatrixPartitionRequest request = builder.build();
    GetMatrixPartitionResponse response = null;
    while (true) {
      try {
        LOG.info("Sending getMatricesPartitionRequest to appMaster ");
        response = masterProxy.getMatrixPartition(null, request);
        if (response.getMatrixStatus() == MatrixStatus.M_NOT_READY) {
          LOG.info("MatrixPartitions are not ready, fetch later!");
          Thread.sleep(1000);
          continue;
        }
        break;
      } catch (ServiceException e) {
        LOG.error("getMatrixPartitions from master failed: " + e);
        // TODO how to exit gracefully
        stop(-1);
      } catch (InterruptedException interruptException) {
        LOG.error("Thread is interrupted!");
        stop(-1);
      }
    }
    List<MatrixPartition> matrisPartitions =
        new ArrayList<MatrixPartition>(response.getMatrixPartitionsCount());
    for (MatrixPartition matrixPartition : response.getMatrixPartitionsList()) {
      matrisPartitions.add(matrixPartition);
      LOG.info("PS get matrixPartitions from master, Id[" + matrixPartition.getMatrixId()
          + "] Name[" + matrixPartition.getMatrixName() + "]");
    }
    return matrisPartitions;
  }
  
  public GetTaskMatrixClockResponse getTaskMatrixClocks() throws ServiceException {
    return masterProxy.getTaskMatrixClocks(null, GetTaskMatrixClockRequest.newBuilder().setPsAttemptId(attemptIdProto).build());
  }

  private void processRecovery() {
    try {
      recoveryFromPreviousSnapshorts();
    } catch (Exception e) {
      LOG.info("Recovery failed, e", e);
    }
  }

  private void recoveryFromPreviousSnapshorts() throws IOException {
    Path snapshots = getPreviousPSSnapshotsPath();
    if (snapshots != null) {
      LOG.info("ps is recovering from hdfs Snapshot. filePath: " + snapshots);
      FSDataInputStream input = fs.open(snapshots, 4096);
      matrixPartitionManager.parseMatricesFromInput(input);
    } else {
      LOG.warn("snapshot file not found, no recovery happened!");
    }

  }
  
  private void adjustMatrixClocks(GetTaskMatrixClockResponse taskMatrixClocks) {
    List<TaskMatrixClock> taskClocks = taskMatrixClocks.getTaskMatrixClocksList();
    int taskNum = taskClocks.size();
    TaskMatrixClock taskMatrixClock = null;
    List<MatrixClock> matrixClocks = null;
    int matrixNum = 0;
    for(int i = 0; i < taskNum; i++){
      taskMatrixClock = taskClocks.get(i);
      matrixClocks = taskMatrixClock.getMatrixClocksList();
      matrixNum = matrixClocks.size();
      for(int j = 0; j < matrixNum; j++){
        LOG.info("task " + taskMatrixClock.getTaskId().getTaskIndex() + "matrix " + matrixClocks.get(j).getMatrixId() + " clock is " + matrixClocks.get(j).getClock());
        matrixPartitionManager.setClock(matrixClocks.get(j).getMatrixId(), taskMatrixClock.getTaskId().getTaskIndex(), matrixClocks.get(j).getClock());
      }
    }
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
   * Gets thread stack.
   *
   * @return the thread stack
   */
  public String getThreadStack()
  {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfo =  threadMXBean.dumpAllThreads(true, true);
    String stackTraceString="ParameterServer\n";
    String infoBlock="\n";
    for(ThreadInfo t :  threadInfo)
    {
      infoBlock="\n\n";
      infoBlock+="threadid: "+t.getThreadId()+"	threadname: "+t.getThreadName()+"		threadstate: "+t.getThreadState()+"\n";
      for(StackTraceElement stackTraceElement : t.getStackTrace())
      {
        infoBlock+= "   "+stackTraceElement.toString()+"\n";
      }
      stackTraceString+=infoBlock+"\n\n";
    }
    return stackTraceString;
  }

}
