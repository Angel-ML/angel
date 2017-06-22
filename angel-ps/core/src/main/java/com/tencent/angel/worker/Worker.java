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

package com.tencent.angel.worker;

import com.google.protobuf.ServiceException;
import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.psagent.CounterUpdater;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.executor.Executor;
import com.tencent.angel.worker.storage.DataBlockManager;
import com.tencent.angel.worker.task.TaskManager;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.WorkerIdProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerCommandProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerRegisterResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Angel Worker,it run a group of {@link com.tencent.angel.worker.task.Task} backed by a thread-pool.
 * The Information is shared by {@link WorkerContext}.
 *@see PSAgent
 *
 */
public class Worker implements Executor {

  private static final Log LOG = LogFactory.getLog(Worker.class);

  private WorkerGroup workerGroup;

  private final Configuration conf;

  private final AtomicBoolean stopped;

  private final boolean isLeader;

  private final WorkerAttemptId workerAttemptId;

  private final WorkerAttemptIdProto workerAttemptIdProto;

  private final Location masterLocation;

  private final TConnection connection;

  private final ApplicationId appId;

  private final String user;

  private final Map<String, String> workerMetrics;

  private TaskManager taskManager;

  private DataBlockManager dataBlockManager;

  private boolean test = false;

  private int initMinClock;

  private Lock readLockForTaskNum;

  private Lock writeLockForTaskNum;

  private int activeTaskNum;

  private final AtomicBoolean workerInitFinishedFlag;

  private Thread heartbeatThread;

  private CounterUpdater counterUpdater;

  private PSAgent psAgent;

  private MasterClient masterClient;

  private final AtomicBoolean exitedFlag;

  private WorkerService workerService;

  /**
   * Instantiates a new Worker.
   *
   * @param conf            the conf
   * @param appId           the app id
   * @param user            the user
   * @param workerAttemptId the worker attempt id
   * @param masterLocation  the master location
   * @param initMinClock    the init min clock
   * @param isLeader        the is leader
   */
  public Worker(Configuration conf, ApplicationId appId, String user,
      WorkerAttemptId workerAttemptId, Location masterLocation, int initMinClock,
      boolean isLeader) {
    this.conf = conf;
    this.stopped = new AtomicBoolean(false);
    this.workerInitFinishedFlag = new AtomicBoolean(false);
    this.exitedFlag = new AtomicBoolean(false);
    this.workerMetrics = new HashMap<String, String>();
    this.connection = TConnectionManager.getConnection(conf);

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLockForTaskNum = readWriteLock.readLock();
    writeLockForTaskNum = readWriteLock.writeLock();

    activeTaskNum = conf.getInt(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM, 1);

    this.appId = appId;
    this.user = user;
    this.masterLocation = masterLocation;
    this.workerAttemptId = workerAttemptId;
    this.isLeader = isLeader;

    this.workerAttemptIdProto = ProtobufUtil.convertToIdProto(workerAttemptId);

    this.initMinClock = initMinClock;
    this.counterUpdater = new CounterUpdater();
    WorkerContext.get().setWorker(this);
  }

  public int getActiveTaskNum() {
    readLockForTaskNum.lock();
    try {
      return activeTaskNum;
    } finally {
      readLockForTaskNum.unlock();
    }
  }

  private void setActiveTaskNum(int taskNum) {
    writeLockForTaskNum.lock();
    try {
      activeTaskNum = taskNum;
    } finally {
      writeLockForTaskNum.unlock();
    }
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
    UserGroupInformation.setConfiguration(conf);

    // set localDir with enviroment set by nm.
    String[] localSysDirs =
        StringUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name()));
    conf.setStrings(AngelConfiguration.LOCAL_DIR, localSysDirs);
    LOG.info(
        AngelConfiguration.LOCAL_DIR + " for child: " + conf.get(AngelConfiguration.LOCAL_DIR));
    int workerGroupIndex = Integer.parseInt(System.getenv(AngelEnvironment.WORKER_GROUP_ID.name()));
    int workerIndex = Integer.parseInt(System.getenv(AngelEnvironment.WORKER_ID.name()));
    int attemptIndex = Integer.parseInt(System.getenv(AngelEnvironment.WORKER_ATTEMPT_ID.name()));

    WorkerGroupId workerGroupId = new WorkerGroupId(workerGroupIndex);
    WorkerId workerId = new WorkerId(workerGroupId, workerIndex);
    WorkerAttemptId workerAttemptId = new WorkerAttemptId(workerId, attemptIndex);

    conf.set(AngelConfiguration.ANGEL_WORKERGROUP_ACTUAL_NUM,
        System.getenv(AngelEnvironment.WORKERGROUP_NUMBER.name()));

    conf.set(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM,
        System.getenv(AngelEnvironment.TASK_NUMBER.name()));
    
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS,
        System.getenv(AngelEnvironment.ANGEL_USER_TASK.name()));

    LOG.info(
        "actual workergroup number:" + conf.get(AngelConfiguration.ANGEL_WORKERGROUP_ACTUAL_NUM));
    LOG.info("actual task number:" + conf.get(AngelConfiguration.ANGEL_TASK_ACTUAL_NUM));

    // get master location
    String masterAddr = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    String portStr = System.getenv(AngelEnvironment.LISTEN_PORT.name());
    Location masterLocation = new Location(masterAddr, Integer.valueOf(portStr));

    String startClock = System.getenv(AngelEnvironment.INIT_MIN_CLOCK.name());
    final Worker worker = new Worker(AngelConfiguration.clone(conf), appId, user, workerAttemptId,
        masterLocation, Integer.valueOf(startClock), false);

    try {
      Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
      UserGroupInformation workerUGI = UserGroupInformation.createRemoteUser(System
        .getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      workerUGI.addCredentials(credentials);

      workerUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          worker.initAndStart();
          return null;
        }
      });
    } catch (Throwable e) {
      LOG.fatal("init and start worker failed ", e);
      worker.error(e.getMessage());
    }
  }


  /**
   * Init and start Worker's components,run tasks through by {@link TaskManager}
   *
   * @throws Exception the exception
   */
  public void initAndStart() throws Exception {
    LOG.info("init and start worker");
    psAgent = new PSAgent(conf, masterLocation.getIp(), masterLocation.getPort(),
        workerAttemptId.getWorkerId().getIndex(), false, this);

    LOG.info("after init psagent");
    dataBlockManager = new DataBlockManager();
    
    LOG.info("after init datablockmanager");
    taskManager = new TaskManager();

    psAgent.initAndStart();
    dataBlockManager.init();

    workerService = new WorkerService();
    workerService.start();


    counterUpdater.initialize();

    // init task manager and start tasks
    masterClient = psAgent.getMasterClient();

    // start heartbeat thread
    startHeartbeatThread();

    workerGroup = masterClient.getWorkerGroupMetaInfo();
    dataBlockManager.setSplitClassification(workerGroup.getSplits());

    taskManager.init();
    taskManager.startAllTasks(
        workerGroup.getWorkerRef(workerAttemptId.getWorkerId()).getTaskIdToContextMap());
    workerInitFinishedFlag.set(true);
  }

  private void startHeartbeatThread() {
    final int heartbeatInterval = conf.getInt(AngelConfiguration.ANGEL_WORKER_HEARTBEAT_INTERVAL,
        AngelConfiguration.DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL);

    heartbeatThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          register();
        } catch (Exception x) {
          LOG.error("register am to rm error:" + x);
          return;
        }

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(heartbeatInterval);
            try {
              heartbeat();
            } catch (YarnRuntimeException e) {
              LOG.error("Error communicating with AM: " + e.getMessage(), e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
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
    heartbeatThread.setName("Worker Heartbeat");
    heartbeatThread.start();
  }

  /**
   * Register to {@link com.tencent.angel.master.AngelApplicationMaster}
   */
  public void register() {
    LOG.info("Register to master");
    try {
      WorkerRegisterResponse response = masterClient.workerRegister();
      if (response.getCommand() == WorkerCommandProto.W_SHUTDOWN) {
        LOG.error("worker received shutdown command when register, to exit now!");
        System.exit(-1);
      }
      LOG.info("worker register finished!");
      // taskManager.assignTaskIds(response.getTaskidsList());
    } catch (Exception x) {
      LOG.fatal("register to appmaster error, worker will exit", x);
      workerExit(-1);
    }
  }

  private void heartbeat() throws IOException, ServiceException {
    try {
      counterUpdater.updateCounters();
      WorkerReportResponse response = masterClient.workerReport();
      assert (response != null);
      switch (response.getCommand()) {
        case W_NEED_REGISTER:
          // todo
          register();
          break;
        case W_SHUTDOWN:
          // if worker timeout, it may be knocked off.
          LOG.fatal("received SHUTDOWN command from am! to exit......");
          System.exit(-1);
          break;
        default:
          int activeTaskNum = response.getActiveTaskNum();
          if (activeTaskNum < getActiveTaskNum()) {
            LOG.warn("Received message that activeTaskNum is changed! oldTaskNum: "
                + getActiveTaskNum() + ", newTaskNum: " + activeTaskNum);
            setActiveTaskNum(activeTaskNum);
          }
          // SUCCESS, do nothing
      }
      // heartbeatFailedTime = 0;
    } catch (Exception netException) {
      if(!stopped.get()) {
        LOG.error("report to appmaster failed, err: ", netException);
      }
    }
  }

  /**
   * Gets task manager.
   *
   * @return the task manager
   */
  public TaskManager getTaskManager() {
    return taskManager;
  }



  /**
   * Notify Master worker is done.
   *
   */
  public void workerDone() {
    if (exitedFlag.compareAndSet(false, true)) {
      LOG.info("worker done");
      try {
        masterClient.workerDone();
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
      workerExit(0);
    }
  }

  /**
   * Notify Master worker has error.
   *
   * @param msg the msg
   */
  public void workerError(String msg) {
    if (exitedFlag.compareAndSet(false, true)) {
      try {
        masterClient.workerError(msg);
        LOG.info("worker failed message : " + msg + ", send it to appmaster success");
      } catch (ServiceException e) {
        LOG.error("send error message error ", e);
      } finally {
        try {
          connection.close();
        } catch (Exception e) {
          LOG.error("close connection error", e);
        }
      }

      workerExit(-1);
    }
  }


  /**
   * Stop Worker
   */
  public void stop() {
    LOG.info("stop workerService");
    if (workerService != null) {
      workerService.stop();
    }

    if (!stopped.getAndSet(true)) {
      LOG.info("stop psagent");
      if (psAgent != null) {
        psAgent.stop();
        psAgent = null;
      }


      LOG.info("stop heartbeat thread");
      if (heartbeatThread != null) {
        heartbeatThread.interrupt();
        try {
          heartbeatThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("Interrupted while stopping.");
        }
        heartbeatThread = null;
      }

      LOG.info("stop taskmanager");
      if (taskManager != null) {
        taskManager.stop();
        taskManager = null;
      }

      LOG.info("stop data block manager");
      if (dataBlockManager != null) {
        dataBlockManager.stop();
        dataBlockManager = null;
      }
    }
  }

  /**
   * Worker exit.
   *
   * @param exitValue the exit value
   */
  public void workerExit(int exitValue) {
    LOG.info("start to close all modules in worker");
    stop();
    exit(exitValue);
  }

  private void exit(int exitValue) {
    AngelDeployMode deployMode = WorkerContext.get().getDeployMode();
    if (deployMode == AngelDeployMode.YARN) {
      System.exit(exitValue);
    }
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
   * Gets data block manager.
   *
   * @return the data block manager
   */
  public DataBlockManager getDataBlockManager() {
    return dataBlockManager;
  }

  /**
   * Gets worker id.
   *
   * @return the worker id
   */
  public WorkerId getWorkerId() {
    return workerAttemptId.getWorkerId();
  }

  /**
   * Gets application id.
   *
   * @return the application id
   */
  public ApplicationId getAppId() {
    return appId;
  }

  /**
   * Gets user.
   *
   * @return the user
   */
  public String getUser() {
    return user;
  }

  /**
   * Gets metrics.
   *
   * @return the metrics
   */
  public Map<String, String> getMetrics() {
    return workerMetrics;
  }

  /**
   * Is test boolean.
   *
   * @return the boolean
   */
  public boolean isTest() {
    return test;
  }

  public void setTest(boolean test) {
    this.test = test;
  }

  /**
   * Gets worker group.
   *
   * @return the worker group
   */
  public WorkerGroup getWorkerGroup() {
    return workerGroup;
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
   * Is leader boolean.
   *
   * @return the boolean
   */
  public boolean isLeader() {
    return isLeader;
  }

  /**
   * Gets worker group id.
   *
   * @return the worker group id
   */
  public WorkerGroupId getWorkerGroupId() {
    return workerAttemptId.getWorkerId().getWorkerGroupId();
  }

  public PSAgent getPSAgent() {
    return psAgent;
  }

  @Override
  public void error(String msg) {
    workerError(msg);
  }

  @Override
  public void done() {
    workerDone();
  }

  @Override
  public int getTaskNum() {
    return workerGroup.getWorkerRef(workerAttemptId.getWorkerId()).getTaskNum();
  }

  /**
   * Gets worker attempt id.
   *
   * @return the worker attempt id
   */
  public WorkerAttemptId getWorkerAttemptId() {
    return workerAttemptId;
  }


  /**
   * Gets worker service.
   *
   * @return the worker service
   */
  public WorkerService getWorkerService() {
    return workerService;
  }

  /**
   * Gets worker attempt id proto.
   *
   * @return the worker attempt id proto
   */
  public WorkerAttemptIdProto getWorkerAttemptIdProto() {
    return workerAttemptIdProto;
  }

  /**
   * Gets worker id proto.
   *
   * @return the worker id proto
   */
  public WorkerIdProto getWorkerIdProto() {
    return workerAttemptIdProto.getWorkerId();
  }

  /**
   * Gets init min clock.
   *
   * @return the init min clock
   */
  public int getInitMinClock() {
    return initMinClock;
  }

  /**
   *Is Worker Initialized
   *
   * @return true if Worker initialization Finished,else false
   */
  public boolean isWorkerInitFinished() {
    return workerInitFinishedFlag.get();
  }
}
