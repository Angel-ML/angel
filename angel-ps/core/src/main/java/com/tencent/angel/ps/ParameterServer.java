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


package com.tencent.angel.ps;

import com.google.protobuf.ServiceException;
import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.matrix.committer.SaveResult;
import com.tencent.angel.ps.metrics.AngelPSAdsMetricsService;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.output.format.SnapshotFormat;
import com.tencent.angel.plugin.AngelServiceLoader;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.PSAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixReportProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSReportRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PSReportResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.PartReportProto;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.RecoverPartKeyProto;
import com.tencent.angel.ps.client.MasterClient;
import com.tencent.angel.ps.client.PSLocationManager;
import com.tencent.angel.ps.io.PSModelIOExecutor;
import com.tencent.angel.ps.io.load.PSModelLoader;
import com.tencent.angel.ps.io.load.SnapshotRecover;
import com.tencent.angel.ps.io.save.PSModelSaver;
import com.tencent.angel.ps.io.save.SnapshotDumper;
import com.tencent.angel.ps.meta.PSMatrixMetaManager;
import com.tencent.angel.ps.metrics.AngelPSMetricsService;
import com.tencent.angel.ps.server.control.ParameterServerService;
import com.tencent.angel.ps.server.data.MatrixTransportServer;
import com.tencent.angel.ps.server.data.PSFailedReport;
import com.tencent.angel.ps.server.data.RunningContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.server.data.profiling.MatrixTransportServerMetrics;
import com.tencent.angel.ps.storage.MatrixStorageManager;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Parameter server,hold and manage individual parameters that divided by {@link
 * com.tencent.angel.master.AngelApplicationMaster}.
 */
public class ParameterServer {

  private static final Log LOG = LogFactory.getLog(ParameterServer.class);
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Application configuration
   */
  private final Configuration conf;

  /**
   * Master location
   */
  private final Location masterLocation;

  /**
   * PS Attempt id
   */
  private final PSAttemptId attemptId;

  /**
   * PS Attempt id proto
   */
  private final PSAttemptIdProto attemptIdProto;

  private final AtomicBoolean stopped;

  /**
   * PS Attempt index
   */
  private final int attemptIndex;

  /**
   * PS RPC server for control message
   */
  private volatile ParameterServerService psServerService;

  /**
   * PS RPC server for data
   */
  private volatile MatrixTransportServer matrixTransportServer;

  /**
   * Heartbeat thread
   */
  private volatile Thread heartbeatThread;

  /**
   * Location manager
   */
  private volatile PSLocationManager locationManager;

  /**
   * Matrix data storage
   */
  private volatile MatrixStorageManager matrixStorageManager;

  /**
   * Matrix meta manager
   */
  private volatile PSMatrixMetaManager matrixMetaManager;


  private volatile PSModelIOExecutor ioExecutor;

  /**
   * Matrix saver
   */
  private volatile PSModelSaver saver;

  /**
   * Matrix loader
   */
  private volatile PSModelLoader loader;

  /**
   * Matrix snapshot dumper
   */
  private volatile SnapshotDumper snapshotDumper;

  /**
   * Master RPC client
   */
  private volatile MasterClient master;

  /**
   * Data collection interval
   */
  private volatile int dataCollectionInterval;

  /**
   * HA update pusher TODO
   */
  // private volatile PS2PSPusherImpl ps2PSPusher;

  /**
   * The RPC handlers for matrix data
   */
  private volatile WorkerPool workerPool;

  private volatile RunningContext runningContext;

  private volatile AngelPSMetricsService metricsService;

  private final PSFailedReport psFailedReport;

  private volatile AngelPSAdsMetricsService adsMetricsService;

  private static final AtomicInteger runningWorkerGroupNum = new AtomicInteger(0);
  private static final AtomicInteger runningWorkerNum = new AtomicInteger(0);
  private static final AtomicInteger runningTaskNum = new AtomicInteger(0);
  private static final AtomicInteger heartbeatCount = new AtomicInteger(0);

  /**
   * Port-check interval in heartbeat thread, defaulting to 30 * default heartbeat interval.
   */
  private volatile long portCheckIntervalMs =
      30L * AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS;

  /**
   * Last successful attempt timestamp of the best-effort heartbeat port check.
   */
  private final AtomicLong lastPortCheckTs = new AtomicLong(0L);

  /**
   * Interval of the RPC-queue backlog log emitted from the heartbeat thread. Kept much
   * shorter than the port check (backlog changes fast and is the thing one watches while a
   * job looks stuck), but still throttled so a 1s heartbeat does not spam the log.
   */
  private volatile long rpcQueueLogIntervalMs =
      3L * AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS;

  /**
   * Last emit timestamp of the heartbeat RPC-queue log.
   */
  private final AtomicLong lastRpcQueueLogTs = new AtomicLong(0L);

  public static int getRunningWorkerGroupNum() {
    return runningWorkerGroupNum.get();
  }

  public static int getRunningWorkerNum() {
    return runningWorkerNum.get();
  }

  public static int getRunningTaskNum() {
    return runningTaskNum.get();
  }

  public static void setRunningWorkerGroupNum(int num) {
    runningWorkerGroupNum.set(num);
  }

  public static void setRunningWorkerNum(int num) {
    runningWorkerNum.set(num);
  }

  public static void setRunningTaskNum(int num) {
    runningTaskNum.set(num);
  }

  /**
   * Create a new Parameter server.
   *
   * @param serverIndex the server index
   * @param attemptIndex the attempt index
   * @param appMasterHost the app master host
   * @param appMasterPort the app master port
   * @param conf the conf
   */
  public ParameterServer(int serverIndex, int attemptIndex, String appMasterHost, int appMasterPort,
      Configuration conf) {
    this.attemptId = new PSAttemptId(new ParameterServerId(serverIndex), attemptIndex);
    this.attemptIdProto = ProtobufUtil.convertToIdProto(attemptId);
    this.attemptIndex = attemptIndex;
    this.conf = conf;
    this.masterLocation = new Location(appMasterHost, appMasterPort);
    this.stopped = new AtomicBoolean(false);
    this.psFailedReport = new PSFailedReport();
    this.context = new PSContext(this);
  }

  /**
   * Gets matrix partition manager.
   *
   * @return the matrix partition manager
   */
  public MatrixStorageManager getMatrixStorageManager() {
    return matrixStorageManager;
  }

  /**
   * Get matrix meta manager
   */
  public PSMatrixMetaManager getMatrixMetaManager() {
    return matrixMetaManager;
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
      psServerService = null;
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

      if (matrixTransportServer != null) {
        try {
          matrixTransportServer.stop();
        } catch (InterruptedException e) {
          LOG.warn("stop matrixTransportServer interrupted.");
        }
        matrixTransportServer = null;
      }

      if (snapshotDumper != null) {
        snapshotDumper.stop();
        snapshotDumper = null;
      }

      if (master != null) {
        master.stop();
        master = null;
      }

      // TODO
      /*
      if(ps2PSPusher != null) {
        ps2PSPusher.stop();
        ps2PSPusher = null;
      }
      */

      if (workerPool != null) {
        workerPool.stop();
        workerPool = null;
      }

      if (ioExecutor != null) {
        ioExecutor.stop();
        ioExecutor = null;
      }

      if (runningContext != null) {
        runningContext.stop();
        runningContext = null;
      }

      if (metricsService != null) {
        metricsService.stop();
        metricsService = null;
      }

      if (adsMetricsService != null) {
        adsMetricsService.stop();
        adsMetricsService = null;
      }

      AngelServiceLoader.stopService();
      exit(exitCode);
    }
  }

  private void exit(int code) {
    AngelDeployMode deployMode = context.getDeployMode();
    if (deployMode == AngelDeployMode.YARN || deployMode == AngelDeployMode.STANDALONE) {
      System.exit(code);
    }
  }

  public static void main(String[] argv) {
    AngelPSShutdownHook hook = null;

    LOG.info("Starting Parameter Server");
    int serverIndex = Integer.valueOf(System.getenv(AngelEnvironment.PARAMETERSERVER_ID.name()));
    String appMasterHost = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    int appMasterPort = Integer.valueOf(System.getenv(AngelEnvironment.LISTEN_PORT.name()));

    int attemptIndex = Integer.valueOf(System.getenv(AngelEnvironment.PS_ATTEMPT_ID.name()));

    Configuration conf = new Configuration();
    conf.addResource(AngelConf.ANGEL_JOB_CONF_FILE);

    conf.setInt("io.file.buffer.size", conf.getInt(AngelConf.ANGEL_PS_IO_FILE_BUFFER_SIZE,
        AngelConf.DEFAULT_ANGEL_PS_IO_FILE_BUFFER_SIZE));

    String user = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation.setConfiguration(conf);

    String runningMode =
        conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE);
    if (runningMode.equals(RunningMode.ANGEL_PS_WORKER.toString())) {
      LOG.debug("AngelEnvironment.TASK_NUMBER.name()=" + AngelEnvironment.TASK_NUMBER.name());
      conf.set(AngelConf.ANGEL_TASK_ACTUAL_NUM, System.getenv(AngelEnvironment.TASK_NUMBER.name()));
    }

    final ParameterServer psServer =
        new ParameterServer(serverIndex, attemptIndex, appMasterHost, appMasterPort, conf);

    try {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      UserGroupInformation psUGI = UserGroupInformation
          .createRemoteUser(System.getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      psUGI.addCredentials(credentials);

      // add a shutdown hook
      hook = new AngelPSShutdownHook(psServer);
      ShutdownHookManager.get().addShutdownHook(hook, SHUTDOWN_HOOK_PRIORITY);

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
    return attemptId.getPsId();
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
    return locationManager.getMasterLocation();
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
   */
  public void initialize() throws IOException, InstantiationException, IllegalAccessException {
    LOG.info("Initialize a parameter server");
    ServerRow.maxLockWaitTimeMs = conf.getInt(AngelConf.ANGEL_PS_MAX_LOCK_WAITTIME_MS,
        AngelConf.DEFAULT_ANGEL_PS_MAX_LOCK_WAITTIME_MS);

    ServerRow.useAdaptiveKey = conf.getBoolean(AngelConf.ANGEL_PS_USE_ADAPTIVE_KEY_ENABLE,
        AngelConf.DEFAULT_ANGEL_PS_USE_ADAPTIVE_KEY_ENABLE);

    ServerRow.useAdaptiveStorage = conf.getBoolean(AngelConf.ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE,
        AngelConf.DEFAULT_ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE);

    ServerRow.sparseToDenseFactor = conf.getFloat(AngelConf.ANGEL_PS_SPARSE_TO_DENSE_FACTOR,
        AngelConf.DEFAULT_ANGEL_PS_SPARSE_TO_DENSE_FACTOR);

    dataCollectionInterval = conf.getInt(AngelConf.ANGEL_PS_HEARTBEAT_DATA_COLLECTION_INTERVAL,
        AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_DATA_COLLECTION_INTERVAL);

    locationManager = new PSLocationManager(context);
    locationManager.setMasterLocation(masterLocation);

    runningContext = new RunningContext(context);
    workerPool = new WorkerPool(context, runningContext);
    workerPool.init();

    ioExecutor = new PSModelIOExecutor(context);
    ioExecutor.init();

    matrixStorageManager = new MatrixStorageManager(context);
    matrixMetaManager = new PSMatrixMetaManager(context);

    master = new MasterClient(context);
    master.init();

    psServerService = new ParameterServerService(context);
    psServerService.start();
    matrixTransportServer = new MatrixTransportServer(getPort() + 1, context);
    // The data port has been held since the control port was chosen; the transport server
    // releases the reservation right before it binds (see MatrixTransportServer#start).
    matrixTransportServer.setPortReservation(psServerService.getPortReservation());

    saver = new PSModelSaver(context);
    loader = new PSModelLoader(context);

    snapshotDumper = new SnapshotDumper(context);

    metricsService = new AngelPSMetricsService(context);

    // The PS resource metrics reporter needs a user supplied backend implementation,
    // so only construct it when explicitly enabled.
    if (conf.getBoolean(AngelConf.ANGEL_METRICS_ADS_ENABLE,
        AngelConf.DEFAULT_ANGEL_METRICS_ADS_ENABLE)) {
      adsMetricsService = new AngelPSAdsMetricsService(context);
    } else {
      LOG.info(AngelConf.ANGEL_METRICS_ADS_ENABLE
          + " is false, skip AngelPSAdsMetricsService initialization.");
    }
  }

  private void startHeartbeat() {
    final int heartbeatInterval = conf.getInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS);
    portCheckIntervalMs = Math.max(heartbeatInterval, heartbeatInterval * 30L);
    rpcQueueLogIntervalMs = Math.max(heartbeatInterval, heartbeatInterval * 3L);
    // Only STANDALONE PSes monitor consecutive heartbeat failures and self-exit on
    // master loss: under YARN the RM restarts the AM, and under LOCAL/EMBEDDED the PS
    // shares the JVM with the AM so this code path is moot.
    final boolean monitorMasterLoss =
        context.getDeployMode() == AngelDeployMode.STANDALONE;
    final int maxConsecutiveFailures = conf.getInt(
        AngelConf.ANGEL_PS_HEARTBEAT_FAILURE_TOLERATE_COUNT,
        AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_FAILURE_TOLERATE_COUNT);
    LOG.info("Starting HeartbeatThread, interval is " + heartbeatInterval + " ms"
        + ", port-check interval is " + portCheckIntervalMs + " ms"
        + ", rpc-queue log interval is " + rpcQueueLogIntervalMs + " ms"
        + (monitorMasterLoss
            ? ", master-loss tolerate count = " + maxConsecutiveFailures
            : ""));
    heartbeatThread = new Thread(() -> {
      // Number of consecutive heartbeat() invocations that have raised an exception.
      // Reset to zero on every successful heartbeat. Used (in STANDALONE only) to
      // self-exit when the master has been unreachable for too long.
      int consecutiveFailures = 0;
      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(heartbeatInterval);
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.warn("Allocated thread interrupted. Returning.", e);
          }
          return;
        }

        try {
          if (!stopped.get()) {
            heartbeat();
            consecutiveFailures = 0;
          }
        } catch (YarnRuntimeException e) {
          LOG.error("Error communicating with AM: " + e.getMessage(), e);
          return;
        } catch (Exception e) {
          LOG.error("ERROR IN CONTACTING RM. ", e);
          if (monitorMasterLoss && maxConsecutiveFailures > 0) {
            consecutiveFailures++;
            if (consecutiveFailures >= maxConsecutiveFailures) {
              // The master appears to be gone. In STANDALONE there is no external
              // supervisor to bring it back, so leaving the PS as an orphan would only
              // hold the listening port and matrix memory hostage. Self-exit so the
              // operator can bring up a clean cluster again.
              LOG.fatal("PS heartbeat to master failed " + consecutiveFailures
                  + " times in a row (>= " + maxConsecutiveFailures
                  + "). Treating master as lost; STANDALONE PS will exit now.");
              // Spawn the shutdown on a separate thread: ParameterServer.stop() joins
              // this very heartbeat thread, which would deadlock if invoked inline.
              final int failedCount = consecutiveFailures;
              Thread killer = new Thread(() ->
                  failed("master lost: " + failedCount + " consecutive heartbeat failures"),
                  "ps-master-loss-killer");
              killer.setDaemon(true);
              killer.start();
              return;
            }
          }
        }
      }
    });
    heartbeatThread.setName("heartbeatThread");
    heartbeatThread.start();
  }

  private void register() {
    try {
      master.register();
      LOG.info("Register to AppMaster successfully");
    } catch (Throwable e) {
      // to exit
      LOG.error("ps register to AppMaster failed: ", e);
      stop(-1);
    }
  }

  private List<MatrixReportProto> buildMatrixReports() {
    MatrixReportProto.Builder matrixBuilder = MatrixReportProto.newBuilder();
    PartReportProto.Builder partBuilder = PartReportProto.newBuilder();
    List<MatrixReportProto> ret = new ArrayList<>();

    for (MatrixMeta matrix : matrixMetaManager.getMatrixMetas().values()) {
      matrixBuilder.setMatrixId(matrix.getId()).setMatrixName(matrix.getName());
      if (context.getPartReplication() > 1) {
        for (PartitionMeta part : matrix.getPartitionMetas().values()) {
          partBuilder.setPartId(part.getPartId()).setStatus(
              context.getMatrixStorageManager().getPart(matrix.getId(), part.getPartId()).getState()
                  .getNumber());
          matrixBuilder.addPartReports(partBuilder.build());
        }
      }
      ret.add(matrixBuilder.build());
      matrixBuilder.clear();
    }
    return ret;
  }

  private void heartbeat() throws Exception {
    checkPortsInHeartbeat();
    logRpcQueueInHeartbeat();

    PSReportRequest.Builder builder = PSReportRequest.newBuilder();
    builder.setPsAttemptId(attemptIdProto);

    if (heartbeatCount.incrementAndGet() % dataCollectionInterval == 0) {

      //calculate data size of all partitions of ps
      Map<Integer, ServerMatrix> serverMatrixMap = matrixStorageManager.getMatrices();
      long dataSize = 0;
      for (ServerMatrix serverMatrix : serverMatrixMap.values()) {
        Map<Integer, ServerPartition> partitions = serverMatrix.getPartitions();
        for (ServerPartition partition : partitions.values()) {
          dataSize += partition.dataSize();
        }
      }

      Pair.Builder pairBuilder = Pair.newBuilder();
      pairBuilder.setKey("key");
      pairBuilder.setValue("value");
      builder.addMetrics(pairBuilder.build());

      // totalRPC
      pairBuilder.setKey("totalRPC");
      pairBuilder.setValue(String.format("%d", runningContext.getTotalRPCCounter()));
      builder.addMetrics(pairBuilder.build());

      // request size
      pairBuilder.setKey("requestSize");
      pairBuilder.setValue(
          String.format("%.2f", runningContext.getRequestDataSize() * 1.0 / 1024 / 1024));
      builder.addMetrics(pairBuilder.build());

      // data size
      pairBuilder.setKey("dataSize");
      pairBuilder.setValue(String.format("%.2f", dataSize * 1.0 / 1024 / 1024));
      builder.addMetrics(pairBuilder.build());
    }
    builder.addAllMatrixReports(buildMatrixReports());

    PSReportResponse ret;
    PSReportRequest request = builder.build();
    LOG.debug("ps hb = " + request);
    ret = master.psReport(request);
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

      default:
        break;
    }

    LOG.debug("ps hb ret = " + ret);
    if (ret.hasNeedSaveMatrices()) {
      saver.save(ProtobufUtil.convert(ret.getNeedSaveMatrices()));
    }

    if (ret.hasNeedLoadMatrices()) {
      loader.load(ProtobufUtil.convert(ret.getNeedLoadMatrices()));
    }
    syncMatrices(ret.getNeedCreateMatricesList(), ret.getNeedReleaseMatrixIdsList(),
        ret.getNeedRecoverPartsList());
  }

  /**
   * Lightweight local liveness probes for PS control/data ports.
   *
   * <p>Best-effort diagnostics only: any failure is logged but MUST NOT fail heartbeat,
   * because heartbeat is the channel used to report status to master.</p>
   */
  private void checkPortsInHeartbeat() {
    long now = System.currentTimeMillis();
    long lastTs = lastPortCheckTs.get();
    if (lastTs > 0 && now - lastTs < portCheckIntervalMs) {
      return;
    }
    if (!lastPortCheckTs.compareAndSet(lastTs, now)) {
      return;
    }

    final int timeoutMs = 1000;
    final String host;
    final int controlPort;
    try {
      host = getHostAddress();
      controlPort = getPort();
    } catch (Throwable e) {
      LOG.info("[PS-HEARTBEAT][PORT-CHECK] resolve host/control port failed", e);
      return;
    }

    checkOnePortInHeartbeat("control", host, controlPort, timeoutMs);
    checkOnePortInHeartbeat("data", host, controlPort + 1, timeoutMs);
  }

  private void checkOnePortInHeartbeat(String portType, String host, int port, int timeoutMs) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(host, port), timeoutMs);
      LOG.info("[PS-HEARTBEAT][PORT-CHECK] " + portType + " ok, host=" + host
          + ", port=" + port + ", timeoutMs=" + timeoutMs);
    } catch (Throwable e) {
      LOG.info("[PS-HEARTBEAT][PORT-CHECK] " + portType + " failed, host=" + host
          + ", port=" + port + ", timeoutMs=" + timeoutMs, e);
    }
  }

  /**
   * Log the RPC dispatch backlog from the heartbeat thread.
   *
   * <p>The detailed per-window queue statistics live in {@code [PS-RPC-METRICS]}, which is
   * off unless {@code angel.matrixtransfer.server.metrics.enable} is set. Backlog is
   * exactly what one needs when a job "hangs", so a compact version is emitted here as
   * well - the heartbeat thread always runs, and this costs a couple of queue size() calls.
   *
   * <p>Best-effort diagnostics only: never allowed to fail the heartbeat.
   */
  private void logRpcQueueInHeartbeat() {
    try {
      long now = System.currentTimeMillis();
      long lastTs = lastRpcQueueLogTs.get();
      if (lastTs > 0 && now - lastTs < rpcQueueLogIntervalMs) {
        return;
      }
      if (!lastRpcQueueLogTs.compareAndSet(lastTs, now)) {
        return;
      }

      LOG.info("[PS-HEARTBEAT][RPC-QUEUE] "
          + MatrixTransportServerMetrics.getInstance().buildQueueSnapshot()
          + " state=" + runningContext.getState()
          + " runningRpc=" + runningContext.getTotalRunningRPCCounter()
          + " totalRpc=" + runningContext.getTotalRPCCounter()
          + String.format(" requestData=%.2fMB",
              runningContext.getRequestDataSize() * 1.0 / 1024 / 1024));
    } catch (Throwable e) {
      LOG.info("[PS-HEARTBEAT][RPC-QUEUE] collect rpc queue info failed", e);
    }
  }

  private void syncMatrices(List<MLProtos.MatrixMetaProto> needCreateMatrices,

      List<Integer> needReleaseMatrices, List<RecoverPartKeyProto> needRecoverParts)
      throws Exception {
    if (!needCreateMatrices.isEmpty()) {
      createMatrices(ProtobufUtil.convertToMatricesMeta(needCreateMatrices));
    }

    if (!needReleaseMatrices.isEmpty()) {
      releaseMatrices(needReleaseMatrices);
    }
  }

  private void createMatrices(List<MatrixMeta> matrixMetas) throws Exception {
    matrixMetaManager.addMatrices(matrixMetas);
    matrixStorageManager.addMatrices(matrixMetas);
    initMatricesData(matrixMetas);
  }

  private void initMatricesData(final List<MatrixMeta> matrixMetas) throws IOException {
    if (context.getPartReplication() > 1 && context.getPSAttemptId().getIndex() > 0) {
      return;
    }

    // Recover PS from snapshot or load path
    if (context.getPSAttemptId().getIndex() > 0) {
      int matrixNum = matrixMetas.size();
      List<PSMatrixLoadContext> matrixLoadContexts = new ArrayList<>(matrixMetas.size());
      SnapshotRecover recover = new SnapshotRecover(context);
      for (int i = 0; i < matrixNum; i++) {
        // 1. First check old snapshot
        Path inputPath = null;
        try {
          inputPath = recover.getSnapshotPath(matrixMetas.get(i).getId());
        } catch (com.tencent.angel.ps.io.load.CorruptedSnapshotException cse) {
          // PHANTOM-SNAPSHOT case: epoch dirs exist but all are empty. Do NOT swallow
          // this the way we swallow generic IOException below — falling through to the
          // master-checkpoint / save-result / InitFunc chain would silently bring up
          // the matrix empty, which for algorithms that load already-trained data is
          // worse than a failed attempt. Rethrow to kill this attempt; the user sees
          // a clear "snapshot corrupted, inspect <hdfs path>" error and can clean up.
          //
          // Behaviour-wise this matches the legacy bug (attempt fails on load), just
          // with a much clearer diagnostic emitted at the recover stage instead of
          // bubbling up from the partition loader several frames deeper.
          throw cse;
        } catch (IOException e) {
          // Generic HDFS / I/O error during the recover probe — keep the historical
          // behaviour of "log and fall through to master checkpoints" so transient NN
          // hiccups don't kill an attempt that could legitimately recover via the
          // master-side bookkeeping.
          LOG.error("Get snapshot path failed, ", e);
        }

        // 2. Check new checkpoints
        if (inputPath == null) {
          try {
            List<SaveResult> saveResults = master.getCheckpoints(matrixMetas.get(i).getId());
            if (saveResults == null || saveResults.isEmpty()) {
              LOG.info("There is no checkpoint results for matrix " + matrixMetas.get(i).getName());
            } else {
              inputPath = new Path(saveResults.get(saveResults.size() - 1).getMatrixPath());
              LOG.info(
                  "There is " + saveResults.size() + " checkpoint results for matrix + "
                      + matrixMetas
                      .get(i).getName()
                      + " we choose the latest result in dir " + saveResults
                      .get(saveResults.size() - 1).getMatrixPath());
            }
          } catch (ServiceException e) {
            LOG.error(
                "Get checkpoint results for matrix " + matrixMetas.get(i).getName() + " failed ",
                e);
          }
        }

        // 3. Check load path setting and old save result
        if (inputPath == null) {
          try {
            List<SaveResult> saveResults = master.getSaveResult(matrixMetas.get(i).getId());
            if (saveResults == null || saveResults.isEmpty()) {
              LOG.info("There is no old save result for matrix " + matrixMetas.get(i).getName());
            } else {
              inputPath = new Path(saveResults.get(saveResults.size() - 1).getMatrixPath());
              LOG.info(
                  "There is " + saveResults.size() + " old save results for matrix + " + matrixMetas
                      .get(i).getName()
                      + " we choose the latest result in dir " + saveResults
                      .get(saveResults.size() - 1).getMatrixPath());
            }
          } catch (ServiceException e) {
            LOG.error("Get save results for matrix " + matrixMetas.get(i).getName() + " failed ",
                e);
          }
        }

        if (inputPath != null) {
          // Belt-and-suspenders: SnapshotRecover.getSnapshotPath already filters out
          // empty / phantom snapshot directories. We re-verify here only as a guard
          // against TOCTOU between the recover stat and the loader stat (e.g. an
          // operator manually deleting the path between the two calls).
          //
          // If the resolved path has actually disappeared by load time, we FAIL the
          // attempt rather than fall through to InitFunc — the user explicitly asked
          // to recover from a saved location, and silently bringing the matrix up
          // empty would risk wiping out trained data (embeddings, GNN weights, etc.).
          // For algorithms that genuinely want "no saved state → init from scratch"
          // the recover stage returns null up front and we never reach this check.
          try {
            FileSystem checkFs = inputPath.getFileSystem(context.getConf());
            if (!checkFs.exists(inputPath)) {
              String msg = "Resolved load path " + inputPath
                  + " for matrix " + matrixMetas.get(i).getName()
                  + " no longer exists at load time (TOCTOU between recover and load)."
                  + " Refusing to fall back to InitFunc / empty matrix to avoid"
                  + " silently zeroing out saved data. Inspect the HDFS state and re-run.";
              LOG.error(msg);
              throw new IOException(msg);
            }
          } catch (IOException e) {
            // Re-throw anything we caught — including our own "path disappeared"
            // IOException — to abort the attempt. A transient HDFS hiccup is rare
            // enough that we'd rather fail loudly than risk a silent demotion.
            throw e;
          }
        }

        if (inputPath != null) {
          LOG.info("Load matrix " + matrixMetas.get(i).getName() + " from " + inputPath.toString());
          matrixLoadContexts.add(new PSMatrixLoadContext(matrixMetas.get(i).getId(),
              inputPath.toString(),
              new ArrayList<>(matrixMetas.get(i).getPartitionMetas().keySet()),
              SnapshotFormat.class.getName()));
        } else {
          // Just init it again
          if (matrixMetas.get(i).getInitFunc() != null) {
            LOG.info("Matrix " + matrixMetas.get(i) + " has a init function " + matrixMetas.get(i)
                .getInitFunc().getClass().getName() + ", use this function to reinit the matrix");
            long startTs = System.currentTimeMillis();
            matrixMetas.get(i).getInitFunc()
                .init(context.getMatrixStorageManager().getMatrix(matrixMetas.get(i).getId()));
            LOG.info("Reinit the matrix use time " + (System.currentTimeMillis() - startTs));
          }
        }
      }

      if (!matrixLoadContexts.isEmpty()) {
        context.getIOExecutors().load(new PSMatricesLoadContext(-1, -1, matrixLoadContexts));
      }
    }
  }

  private void releaseMatrices(List<Integer> matrixIds) {
    if (!matrixIds.isEmpty()) {
      matrixMetaManager.removeMatrices(matrixIds);
      clearMatricesData(matrixIds);
    }
  }

  private void clearMatricesData(List<Integer> matrixIds) {
    matrixStorageManager.removeMatrices(matrixIds);
  }

  /**
   * Start parameter server services.
   *
   * @throws IOException the io exception
   */
  public void start() throws Exception {
    int rpcWorkerPoolSize = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE);
    int nettyServerEventGroupThreads = conf.getInt(
        AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM);
    int nettyClientEventGroupThreads = conf.getInt(
        AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM);
    int matrixDiskIoThreads = conf.getInt(
        AngelConf.ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE);
    LOG.info("[PS-THREAD-POOL] startup summary: rpcWorkerPoolSize=" + rpcWorkerPoolSize
        + ", nettyServerEventGroupThreads=" + nettyServerEventGroupThreads
        + ", nettyClientEventGroupThreads=" + nettyClientEventGroupThreads
        + ", matrixDiskIoThreads=" + matrixDiskIoThreads);

    // Init and start metrics reporter
    metricsService.init();

    metricsService.start();

    if (adsMetricsService != null) {
      adsMetricsService.start();
    }

    if (snapshotDumper != null) {
      snapshotDumper.start();
    }
    master.start();

    // TODO
    // if(ps2PSPusher != null) {
    // ps2PSPusher.start();
    // }

    workerPool.start();
    ioExecutor.start();
    matrixTransportServer.start();
    runningContext.start();

    if (getAttemptIndex() > 0) {
      LOG.info("PS " + getServerId() + " running attempt " + getAttemptIndex()
          + " load matrices from snapshot if need");
      List<MatrixMeta> matrixMetas = master.getMatricesMeta();
      if (!matrixMetas.isEmpty()) {
        createMatrices(matrixMetas);
      }
    }

    register();
    startHeartbeat();
    AngelServiceLoader.startServiceIfNeed(this, getConf());
  }

  /**
   * Done, will notify master and exit
   */
  public void done() {
    try {
      master.done();
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
      master.failed(errorLog);
      LOG.info("send failed message to master success");
    } catch (ServiceException e) {
      LOG.error("send failed message to master failed ", e);
    } finally {
      stop(-1);
    }
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
   *
   * @return MasterProtocol rpc client to master
   */
  public MasterClient getMaster() {
    return master;
  }

  /**
   * Get attempt index
   *
   * @return attempt index
   */
  public int getAttemptIndex() {
    return attemptIndex;
  }

  /**
   * Get location manager
   *
   * @return location manager
   */
  public PSLocationManager getLocationManager() {
    return locationManager;
  }

  /**
   * TODO
   * Get PS 2 PS update pusher
   * @return PS 2 PS update pusher
   */
  // public PS2PSPusherImpl getPs2PSPusher() {
  // return ps2PSPusher;
  // }

  /**
   * Get RPC worker pool
   *
   * @return RPC worker pool
   */
  public WorkerPool getWorkerPool() {
    return workerPool;
  }

  /**
   * Get File Read/Writer executors
   *
   * @return File Read/Writer executors
   */
  public PSModelIOExecutor getPSModelIOExecutor() {
    return ioExecutor;
  }

  /**
   * Get Snapshot dumper
   *
   * @return Snapshot dumper
   */
  public SnapshotDumper getSnapshotDumper() {
    return snapshotDumper;
  }

  /**
   * Get PS failed information reporter
   *
   * @return PS failed information reporter
   */
  public PSFailedReport getPSFailedReport() {
    return psFailedReport;
  }

  /**
   * Get PS running context
   *
   * @return PS running context
   */
  public RunningContext getRunningContext() {
    return runningContext;
  }

  public AngelPSMetricsService getMetricsService() {
    return metricsService;
  }

  public PSContext getContext() {
    return context;
  }

  static class AngelPSShutdownHook implements Runnable {
    ParameterServer ps;

    AngelPSShutdownHook(ParameterServer ps) {
      this.ps = ps;
    }

    public void run() {
      LOG.info("PS received a signal. stop the ps");
      ps.stop(0);
    }
  }
}
