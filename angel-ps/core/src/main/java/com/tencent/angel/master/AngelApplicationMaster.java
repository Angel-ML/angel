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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.master;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.master.LocationManager;
import com.tencent.angel.master.MasterService;
import com.tencent.angel.master.MatrixMetaManager;
import com.tencent.angel.master.app.*;
import com.tencent.angel.master.data.DataSpliter;
import com.tencent.angel.master.data.DummyDataSpliter;
import com.tencent.angel.master.deploy.ContainerAllocator;
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEventType;
import com.tencent.angel.master.deploy.local.LocalContainerAllocator;
import com.tencent.angel.master.deploy.local.LocalContainerLauncher;
import com.tencent.angel.master.deploy.yarn.YarnContainerAllocator;
import com.tencent.angel.master.deploy.yarn.YarnContainerLauncher;
import com.tencent.angel.master.oplog.AppStateStorage;
import com.tencent.angel.master.ps.*;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.master.ps.ps.AMParameterServerEvent;
import com.tencent.angel.master.ps.ps.AMParameterServerEventType;
import com.tencent.angel.master.psagent.*;
import com.tencent.angel.master.slowcheck.SlowChecker;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.master.worker.WorkerManagerEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.master.worker.worker.AMWorkerEvent;
import com.tencent.angel.master.worker.worker.AMWorkerEventType;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupEvent;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.webapp.AngelWebApp;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.master.task.AMTaskManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

/**
 * Angel application master. It contains service modules: worker manager, parameter server manager,
 * container allocator, container launcher, task manager and event handler.
 * 
 */
public class AngelApplicationMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(AngelApplicationMaster.class);
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /** application name */
  private final String appName;

  /** application configuration */
  private final Configuration conf;

  /** application attempt id */
  private final ApplicationAttemptId appAttemptId;

  /** app start time */
  private final long startTime;

  /** app submit time */
  private final long appSubmitTime;

  /** container id for angel application master */
  private final ContainerId containerId;

  /** The host of the node manager where the angel applitaion master is located */
  private final String nmHost;

  /** The port of the node manager where the angel applitaion master is located */
  private final int nmPort;

  /** The web port of the node manager where the angel applitaion master is located */
  private final int nmHttpPort;

  /** angel application master credentials */
  private final Credentials credentials;

  /** application running context, it is used to share information between all service module */
  private final AMContext appContext;

  /** system clock */
  private final SystemClock clock;

  /** event dispatcher */
  private Dispatcher dispatcher;

  /** container allocator, it used to apply running resource for workers and parameter servers */
  private ContainerAllocator containerAllocator;

  /** container allocator, it used to launch workers and parameter servers */
  private ContainerLauncher containerLauncher;

  /** parameter server manager */
  private ParameterServerManager psManager;

  /**
   * angel application master service, it is used to response RPC request from client, workers and
   * parameter servers
   */
  private MasterService masterService;

  /** matrix meta manager */
  private MatrixMetaManager matrixMetaManager;

  /** parameter server location manager */
  private LocationManager locationManager;

  /** worker manager */
  private WorkerManager workerManager;

  /** it use to split train data */
  private DataSpliter dataSpliter;

  /** angel application master state storage */
  private AppStateStorage appStateStorage;

  /** angel application state */
  private final App angelApp;

  /** a web service for http access */
  private WebApp webApp;

  /** psagent manager */
  private PSAgentManager psAgentManager;

  /** identifies whether the temporary resource is cleared */
  private boolean isCleared;

  /** task manager */
  private AMTaskManager taskManager;

  private final Lock lock;

  public AngelApplicationMaster(Configuration conf, String appName,
      ApplicationAttemptId applicationAttemptId, ContainerId containerId, String nmHost, int nmPort,
      int nmHttpPort, long appSubmitTime, Credentials credentials) throws IllegalArgumentException, IOException {
    super(AngelApplicationMaster.class.getName());
    this.conf = conf;
    this.appName = appName;
    this.appAttemptId = applicationAttemptId;
    this.appSubmitTime = appSubmitTime;
    this.containerId = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;

    this.clock = new SystemClock();
    this.startTime = clock.getTime();
    this.isCleared = false;
    this.credentials = credentials;

    appContext = new RunningAppContext(conf);
    angelApp = new App(appContext);
    lock = new ReentrantLock();
  }

  /**
   * running application master context
   */
  public class RunningAppContext implements AMContext {
    private final ClientToAMTokenSecretManager clientToAMTokenSecretManager;

    public RunningAppContext(Configuration config) {
      this.clientToAMTokenSecretManager = new ClientToAMTokenSecretManager(appAttemptId, null);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptId;
    }

    @Override
    public MasterService getMasterService() {
      return masterService;
    }

    @Override
    public ApplicationId getApplicationId() {
      return appAttemptId.getApplicationId();
    }

    @Override
    public String getApplicationName() {
      return appName;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    @Override
    public String getUser() {
      return conf.get(AngelConfiguration.USER_NAME);
    }

    @Override
    public Clock getClock() {
      return clock;
    }

    @Override
    public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
      return clientToAMTokenSecretManager;
    }

    @Override
    public Credentials getCredentials() {
      return credentials;
    }

    @Override
    public ContainerAllocator getContainerAllocator() {
      return containerAllocator;
    }

    @Override
    public ParameterServerManager getParameterServerManager() {
      return psManager;
    }

    @Override
    public Dispatcher getDispatcher() {
      return dispatcher;
    }

    @Override
    public App getApp() {
      return angelApp;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public WebApp getWebApp() {
      return webApp;
    }

    @Override
    public MatrixMetaManager getMatrixMetaManager() {
      return matrixMetaManager;
    }

    @Override
    public LocationManager getLocationManager() {
      return locationManager;
    }

    @Override
    public RunningMode getRunningMode() {
      String mode = conf.get(AngelConfiguration.ANGEL_RUNNING_MODE,
          AngelConfiguration.DEFAULT_ANGEL_RUNNING_MODE);
      if (mode.equals(RunningMode.ANGEL_PS.toString())) {
        return RunningMode.ANGEL_PS;
      } else if (mode.equals(RunningMode.ANGEL_PS_PSAGENT.toString())) {
        return RunningMode.ANGEL_PS_PSAGENT;
      } else {
        return RunningMode.ANGEL_PS_WORKER;
      }
    }

    @Override
    public PSAgentManager getPSAgentManager() {
      return psAgentManager;
    }

    @Override
    public WorkerManager getWorkerManager() {
      return workerManager;
    }

    @Override
    public DataSpliter getDataSpliter() {
      return dataSpliter;
    }

    @Override
    public int getTotalIterationNum() {
      return conf.getInt("ml.epoch.num", AngelConfiguration.DEFAULT_ANGEL_TASK_ITERATION_NUMBER);
    }

    @Override
    public AMTaskManager getTaskManager() {
      return taskManager;
    }

    @Override
    public int getAMAttemptTime() {
      return conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    }

    @Override
    public AppStateStorage getAppStateStorage() {
      return appStateStorage;
    }

    @Override
    public boolean needClear() {
      return !getApp().isShouldRetry() || appAttemptId.getAttemptId() >= getAMAttemptTime() || angelApp.isSuccess();

    }

    @Override
    public AngelDeployMode getDeployMode() {
      String mode = conf.get(AngelConfiguration.ANGEL_DEPLOY_MODE,
          AngelConfiguration.DEFAULT_ANGEL_DEPLOY_MODE);
      if (mode.equals(AngelDeployMode.LOCAL.toString())) {
        return AngelDeployMode.LOCAL;
      } else {
        return AngelDeployMode.YARN;
      }
    }
  }

  public void clear() throws IOException {
    boolean deleteSubmitDir =
        appContext.getConf().getBoolean(AngelConfiguration.ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE,
            AngelConfiguration.DEFAULT_ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE);
    if (deleteSubmitDir) {
      cleanupStagingDir();
    }

    cleanTmpOutputDir();
  }

  private void cleanTmpOutputDir() {
    Configuration conf = appContext.getConf();
    String tmpOutDir = conf.get(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY);
    if (tmpOutDir == null) {
      return;
    }

    try {
      LOG.info("Deleting tmp output directory " + tmpOutDir);
      Path tmpOutPath = new Path(tmpOutDir);
      FileSystem fs = tmpOutPath.getFileSystem(conf);
      fs.delete(tmpOutPath, true);
    } catch (IOException io) {
      LOG.error("Failed to cleanup staging dir " + tmpOutDir, io);
    }
  }

  private void cleanupStagingDir() throws IOException {
    Configuration conf = appContext.getConf();
    String stagingDir = conf.get(AngelConfiguration.ANGEL_JOB_DIR);
    if (stagingDir == null) {
      LOG.warn("App Staging directory is null");
      return;
    }

    try {
      Path stagingDirPath = new Path(stagingDir);
      FileSystem fs = stagingDirPath.getFileSystem(conf);
      LOG.info("Deleting staging directory " + FileSystem.getDefaultUri(conf) + " " + stagingDir);
      fs.delete(stagingDirPath, true);
    } catch (IOException io) {
      LOG.error("Failed to cleanup staging dir " + stagingDir, io);
    }
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  /**
   * stop all services of angel application master and clear tmp directory
   */
  public void shutDownJob() {
    try {
      lock.lock();
      if (isCleared) {
        return;
      }

      // stop all services
      LOG.info("Calling stop for all the services");
      AngelApplicationMaster.this.stop();

      // 1.write application state to file so that the client can get the state of the application
      // if master exit
      // 2.clear tmp and staging directory
      if (appContext.needClear()) {
        LOG.info("start to write app state to file and clear tmp directory");
        writeAppState();
        clear();
      }

      // waiting for client to get application state
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.warn("ShutDownjob error ", e);
      }

      // stop the RPC server
      masterService.stop();
    } catch (Throwable t) {
      LOG.warn("Graceful stop failed ", t);
    } finally {
      isCleared = true;
      lock.unlock();
    }

    LOG.info("Exiting Angel AppMaster..GoodBye!");

    exit(0);
  }

  private void exit(int code) {
    AngelDeployMode deployMode = appContext.getDeployMode();
    if (deployMode == AngelDeployMode.YARN) {
      System.exit(code);
    }
  }

  private void writeAppState() throws IllegalArgumentException, IOException {
    String interalStatePath =
        appContext.getConf().get(AngelConfiguration.ANGEL_APP_SERILIZE_STATE_FILE);

    LOG.info("start to write app state to file " + interalStatePath);

    if (interalStatePath == null) {
      LOG.error("can not find app state serilize file, exit");
      return;
    }

    Path stateFilePath = new Path(interalStatePath);
    FileSystem fs = stateFilePath.getFileSystem(appContext.getConf());
    if (fs.exists(stateFilePath)) {
      fs.delete(stateFilePath, false);
    }

    FSDataOutputStream out = fs.create(stateFilePath);
    appContext.getApp().serilize(out);
    out.flush();
    out.close();

    LOG.info("write app state over");
  }

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    AngelAppMasterShutdownHook hook = null;
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());

      String containerIdStr = System.getenv(Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(Environment.NM_HOST.name());
      String nodePortString = System.getenv(Environment.NM_PORT.name());
      String nodeHttpPortString = System.getenv(Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr = System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String maxAppAttempts = System.getenv(ApplicationConstants.MAX_APP_ATTEMPTS_ENV);

      validateInputParam(containerIdStr, Environment.CONTAINER_ID.name());
      validateInputParam(nodeHostString, Environment.NM_HOST.name());
      validateInputParam(nodePortString, Environment.NM_PORT.name());
      validateInputParam(nodeHttpPortString, Environment.NM_HTTP_PORT.name());
      validateInputParam(appSubmitTimeStr, ApplicationConstants.APP_SUBMIT_TIME_ENV);
      validateInputParam(maxAppAttempts, ApplicationConstants.MAX_APP_ATTEMPTS_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      Configuration conf = new Configuration();
      conf.addResource(AngelConfiguration.ANGEL_JOB_CONF_FILE);

      String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());
      conf.set(AngelConfiguration.USER_NAME, jobUserName);
      conf.setBoolean("fs.automatic.close", false);

      UserGroupInformation.setConfiguration(conf);

      // Security framework already loaded the tokens into current UGI, just use
      // them
      Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
      LOG.info("Executing with tokens:");
      for (Token<?> token : credentials.getAllTokens()) {
        LOG.info(token);
      }

      UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
      appMasterUgi.addCredentials(credentials);

      // Now remove the AM->RM token so tasks don't have it
      Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        Token<?> token = iter.next();
        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          iter.remove();
        }
      }

      String appName = conf.get(AngelConfiguration.ANGEL_JOB_NAME);

      LOG.info("app name=" + appName);
      LOG.info("app attempt id=" + applicationAttemptId);

      final AngelApplicationMaster appMaster = new AngelApplicationMaster(conf, appName,
          applicationAttemptId, containerId, nodeHostString, Integer.parseInt(nodePortString),
          Integer.parseInt(nodeHttpPortString), appSubmitTime, credentials);

      // add a shutdown hook
      hook = new AngelAppMasterShutdownHook(appMaster);
      ShutdownHookManager.get().addShutdownHook(hook, SHUTDOWN_HOOK_PRIORITY);

      appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          appMaster.initAndStart();
          return null;
        }
      });
    } catch (Throwable t) {
      LOG.fatal("Error starting AppMaster", t);
      if(hook != null) {
        ShutdownHookManager.get().removeShutdownHook(hook);
      }
      System.exit(1);
    }
  }

  private static void validateInputParam(String value, String param) throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * init and start all service modules for angel applicaiton master.
   */
  public void initAndStart() throws IOException {
    addIfService(angelApp);

    // init app state storage
    String tmpOutPath = conf.get(AngelConfiguration.ANGEL_JOB_TMP_OUTPUT_DIRECTORY);
    Path appStatePath = new Path(tmpOutPath, "app");
    LOG.info("app state output path = " + appStatePath.toUri().toString());
    FileSystem fs = appStatePath.getFileSystem(conf);
    appStateStorage = new AppStateStorage(appContext, appStatePath.toUri().toString(), fs);
    addIfService(appStateStorage);
    LOG.info("build app state storage success");

    // init event dispacher
    dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);
    LOG.info("build event dispacher");

    // init location manager
    locationManager = new LocationManager(appContext);

    // init container allocator
    AngelDeployMode deployMode = appContext.getDeployMode();
    LOG.info("deploy mode=" + deployMode);
    if (deployMode == AngelDeployMode.LOCAL) {
      containerAllocator = new LocalContainerAllocator(appContext);
      containerLauncher = new LocalContainerLauncher(appContext);
    } else {
      containerAllocator = new YarnContainerAllocator(appContext);
      containerLauncher = new YarnContainerLauncher(appContext);
    }
    addIfService(containerAllocator);
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
    LOG.info("build containerAllocator success");
    addIfService(containerLauncher);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);
    LOG.info("build containerLauncher success");

    // init a rpc service
    masterService = new MasterService(appContext);
    LOG.info("build master service success");

    // recover matrix meta if needed
    recoverMatrixMeta();

    // recover ps attempt information if need
    Map<ParameterServerId, Integer> psIdToAttemptIndexMap = recoverPSAttemptIndex();
    if (psIdToAttemptIndexMap == null) {
      LOG.info("recoverPSAttemptIndex return is null");
    } else {
      for (Entry<ParameterServerId, Integer> entry : psIdToAttemptIndexMap.entrySet()) {
        LOG.info("psId=" + entry.getKey() + ",attemptIndex=" + entry.getValue());
      }
    }

    // init parameter server manager
    psManager = new ParameterServerManager(appContext, psIdToAttemptIndexMap);
    addIfService(psManager);
    dispatcher.register(ParameterServerManagerEventType.class, psManager);
    dispatcher.register(AMParameterServerEventType.class, new ParameterServerEventHandler());
    dispatcher.register(PSAttemptEventType.class, new PSAttemptEventDispatcher());
    LOG.info("build PSManager success");

    // recover task information if needed
    recoverTaskState();

    RunningMode mode = appContext.getRunningMode();
    LOG.info("running mode=" + mode);
    switch (mode) {
      case ANGEL_PS_PSAGENT: {
        // init psagent manager and register psagent manager event
        psAgentManager = new PSAgentManager(appContext);
        addIfService(psAgentManager);
        dispatcher.register(PSAgentManagerEventType.class, psAgentManager);
        dispatcher.register(AMPSAgentEventType.class, new PSAgentEventHandler());
        dispatcher.register(PSAgentAttemptEventType.class, new PSAgentAttemptEventHandler());
        LOG.info("build PSAgentManager success");
        break;
      }

      case ANGEL_PS_WORKER: {
        // a dummy data spliter is just for test now
        boolean useDummyDataSpliter =
            conf.getBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER,
                AngelConfiguration.DEFAULT_ANGEL_AM_USE_DUMMY_DATASPLITER);
        if (useDummyDataSpliter && deployMode == AngelDeployMode.LOCAL) {
          dataSpliter = new DummyDataSpliter(appContext);
        } else {
          // recover data splits information if needed
          recoveryDataSplits();
        }

        // init worker manager and register worker manager event
        workerManager = new WorkerManager(appContext);
        workerManager.adjustTaskNumber(dataSpliter.getSplitNum());
        addIfService(workerManager);
        dispatcher.register(WorkerManagerEventType.class, workerManager);
        dispatcher.register(AMWorkerGroupEventType.class, new WorkerGroupEventHandler());
        dispatcher.register(AMWorkerEventType.class, new WorkerEventHandler());
        dispatcher.register(WorkerAttemptEventType.class, new WorkerAttemptEventHandler());
        LOG.info("build WorkerManager success");
        break;
      }

      case ANGEL_PS:
        break;
    }

    // register slow worker/ps checker
    addIfService(new SlowChecker(appContext));

    // register app manager event and finish event
    dispatcher.register(AppEventType.class, angelApp);
    dispatcher.register(AppFinishEventType.class, new AppFinishEventHandler());

    try {
      masterService.init(conf);
      super.init(conf);

      // start a web service if use yarn deploy mode
      if (deployMode == AngelDeployMode.YARN) {
        try {
          webApp = WebApps.$for("angel", AMContext.class, appContext).with(conf)
              .start(new AngelWebApp());
          LOG.info("start webapp server success");
          LOG.info("webApp.port()=" + webApp.port());
        } catch (Exception e) {
          LOG.error("Webapps failed to start. Ignoring for now:", e);
        }
      }

      masterService.start();
      super.serviceStart();    
      psManager.startAllPS();
      
      LOG.info("appAttemptId.getAttemptId()=" + appAttemptId.getAttemptId());
      if (appAttemptId.getAttemptId() > 1) {
        angelApp.startExecute();
      }
    } catch (Exception e) {
      LOG.error("Failed startup APPMaster.", e);
    }
  }

  private void recoverMatrixMeta() {
    // load from app state storage first if attempt index great than 1(the master is not the first
    // retry)
    if (appAttemptId.getAttemptId() > 1) {
      try {
        matrixMetaManager = appStateStorage.loadMatrixMeta();
      } catch (Exception e) {
        LOG.error("load matrix meta from file failed.", e);
      }
    }

    // if load failed, just build a new MatrixMetaManager
    if (matrixMetaManager == null) {
      matrixMetaManager = new MatrixMetaManager();
    }
  }

  private Map<ParameterServerId, Integer> recoverPSAttemptIndex() {
    // load ps attempt index from app state storage first if attempt index great than 1(the master
    // is not the first retry)
    Map<ParameterServerId, Integer> psIdToAttemptIndexMap = null;
    if (appAttemptId.getAttemptId() > 1) {
      try {
        psIdToAttemptIndexMap = appStateStorage.loadPSMeta();
      } catch (Exception e) {
        LOG.error("load task meta from file failed.", e);
      }
    }

    return psIdToAttemptIndexMap;
  }

  private void recoverTaskState() throws IOException {
    // load task information from app state storage first if attempt index great than 1(the master
    // is not the first retry)
    if (appAttemptId.getAttemptId() > 1) {
      try {
        taskManager = appStateStorage.loadTaskMeta();
      } catch (Exception e) {
        LOG.error("load task meta from file failed.", e);
      }
    }

    // if load failed, just build a new AMTaskManager
    if (taskManager == null) {
      taskManager = new AMTaskManager();
    }
  }


  private void recoveryDataSplits() throws IOException {
    // load data splits information from app state storage first if attempt index great than 1(the
    // master is not the first retry)
    if (appAttemptId.getAttemptId() > 1) {
      try {
        dataSpliter = appStateStorage.loadDataSplits();
      } catch (Exception e) {
        LOG.error("load split from split file failed.", e);
      }
    }

    // if load failed, we need to recalculate the data splits
    if (dataSpliter == null) {
      try {
        dataSpliter = new DataSpliter(appContext);
        dataSpliter.generateSplits();
        appStateStorage.writeDataSplits(dataSpliter);
      } catch (Exception x) {
        LOG.error("Split failed, error: " + x);
        throw new IOException(x);
      }
    }

    if(dataSpliter.getSplitNum() == 0) {
      throw new IOException("training data directory is empty");
    }
  }

  public AMContext getAppContext() {
    return appContext;
  }

  public String getAppName() {
    return appName;
  }

  public long getAppSubmitTime() {
    return appSubmitTime;
  }

  public ApplicationAttemptId getAppAttemptId() {
    return appAttemptId;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public String getNmHost() {
    return nmHost;
  }

  public int getNmPort() {
    return nmPort;
  }

  public int getNmHttpPort() {
    return nmHttpPort;
  }

  public class WorkerAttemptEventHandler implements EventHandler<WorkerAttemptEvent> {

    @Override
    public void handle(WorkerAttemptEvent event) {
      WorkerAttemptId workerAttemptId = event.getWorkerAttemptId();

      workerManager.getWorker(workerAttemptId.getWorkerId()).getWorkerAttempt(workerAttemptId)
          .handle(event);
    }
  }

  public class WorkerEventHandler implements EventHandler<AMWorkerEvent> {

    @Override
    public void handle(AMWorkerEvent event) {
      workerManager.getWorker(event.getWorkerId()).handle(event);
    }
  }

  public class WorkerGroupEventHandler implements EventHandler<AMWorkerGroupEvent> {

    @Override
    public void handle(AMWorkerGroupEvent event) {
      workerManager.getWorkerGroup(event.getGroupId()).handle(event);
    }
  }

  public class ParameterServerEventHandler implements EventHandler<AMParameterServerEvent> {

    @Override
    public void handle(AMParameterServerEvent event) {
      ParameterServerId id = event.getPsId();
      AMParameterServer server = psManager.getParameterServer(id);
      server.handle(event);
    }
  }

  public class PSAttemptEventDispatcher implements EventHandler<PSAttemptEvent> {

    @Override
    public void handle(PSAttemptEvent event) {
      PSAttemptId attemptId = event.getPSAttemptId();
      ParameterServerId id = attemptId.getParameterServerId();
      psManager.getParameterServer(id).getPSAttempt(attemptId).handle(event);
    }
  }

  public class PSAgentEventHandler implements EventHandler<AMPSAgentEvent> {

    @Override
    public void handle(AMPSAgentEvent event) {
      psAgentManager.getPsAgent(event.getId()).handle(event);
    }
  }

  public class PSAgentAttemptEventHandler implements EventHandler<PSAgentAttemptEvent> {

    @Override
    public void handle(PSAgentAttemptEvent event) {
      psAgentManager.getPsAgent(event.getId().getPsAgentId()).getAttempt(event.getId())
          .handle(event);
    }
  }

  public class AppFinishEventHandler implements EventHandler<AppFinishEvent> {
    @Override
    public void handle(AppFinishEvent event) {
      switch (event.getType()) {
        case INTERNAL_ERROR_FINISH:
        case SUCCESS_FINISH:
        case KILL_FINISH: {
          new Thread() {
            @Override
            public void run() {
              shutDownJob();
            }
          }.start();
        }
      }
    }
  }

  static class AngelAppMasterShutdownHook implements Runnable {
    AngelApplicationMaster appMaster;

    AngelAppMasterShutdownHook(AngelApplicationMaster appMaster) {
      this.appMaster = appMaster;
    }

    public void run() {
      LOG.info("AM received a signal. stop the app");
      appMaster.getAppContext().getApp().addDiagnostics("killed");
      appMaster.getAppContext().getApp().forceState(AppState.KILLED);
      appMaster.shutDownJob();
    }
  }
}
