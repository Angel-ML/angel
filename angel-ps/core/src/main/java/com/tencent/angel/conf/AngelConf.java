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

package com.tencent.angel.conf;

import com.tencent.angel.RunningMode;
import com.tencent.angel.data.inputformat.BalanceInputFormat;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.slowcheck.TaskCalPerfChecker;
import com.tencent.angel.ps.impl.ParameterServer;
import com.tencent.angel.ps.impl.matrix.DefaultRowUpdater;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.matrix.cache.DefaultPolicy;
import com.tencent.angel.utils.DefaultAppSubmitter;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.task.BaseTask;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Properties;

/**
 * Angel system parameters.
 */
public class AngelConf extends Configuration {

  public AngelConf(Configuration conf) {
    super(conf);
  }

  private static final String ANGEL_DEFAULT_XML_FILE = "angel-default.xml";
  private static final String ANGEL_SITE_XML_FILE = "angel-site.xml";

  private static final String ANGEL_PREFIX = "angel.";
  private static final String ANGEL_AM_PREFIX = "angel.am.";
  private static final String ANGEL_WORKER_PREFIX = "angel.worker.";
  private static final String ANGEL_PS_PREFIX = "angel.ps.";
  private static final String ANGEL_TASK_PREFIX = "angel.task.";
  private static final String ANGEL_WORKERGROUP_PREFIX = "angel.workergroup.";

  // //////////////////////////////
  // Application Configs
  // //////////////////////////////

  /**
   * Task action type. There are two action types now:train and predict.
   * <p>
   * "train" action type means training model use training data.
   * <p>
   * "predict" action type means predict result use model.
   */
  public static final String ANGEL_ACTION_TYPE = "action.type";
  public static final String DEFAULT_ANGEL_ACTION_TYPE = "train";

  /** Training data path. */
  public static final String ANGEL_TRAIN_DATA_PATH = "angel.train.data.path";

  /** Predict data path. */
  public static final String ANGEL_PREDICT_DATA_PATH = "angel.predict.data.path";

  /** Input data path use by Angel */
  public static final String ANGEL_JOB_INPUT_PATH = "angel.job.input.path";

  /** Training data file format. */
  public static final String ANGEL_INPUTFORMAT_CLASS = ANGEL_PREFIX + "input.format";
  public static final String DEFAULT_ANGEL_INPUTFORMAT_CLASS = BalanceInputFormat.class
      .getName();

  /** Predict result output path. If use "predict" action, we need set it. */
  public static final String ANGEL_PREDICT_PATH = "angel.predict.out.path";

  /** Serving temp output path. If use "serving" action, we need set it. */
  public static final String ANGEL_SERVING_TEMP_PATH = "angel.serving.temp.path";

  /** Serving client type. If use "serving" action, we need set it. */
  public static final String ANGEL_SERVING_CLIENT_TYPE = "angel.serving.client.type";

  /** Model save path. This parameter is used in "train" action. */
  public static final String ANGEL_SAVE_MODEL_PATH = "angel.save.model.path";

  /**
   * Log save path. This parameter is used in "train" action, each iteration outputs some algorithm
   * indicators to this file
   */
  public static final String DEFAULT_METRIC_FORMAT = "%10.6e";
  public static final String ANGEL_LOG_PATH = "angel.log.path";
  public static final String ANGEL_LOG_FAST_WRITE = "angel.log.fast.write.enable";
  public static final Boolean DEFAULT_ANGEL_LOG_FAST_WRITE = true;
  public static final String ANGEL_LOG_FLUSH_MIN_SIZE = "angel.log.flush.min.size";
  public static final int DEFAULT_ANGEL_LOG_FLUSH_MIN_SIZE = 8;

  /**
   * Model load path. This parameter is used in both "train" and "predict" actions. In "train"
   * action, we can load old model from file to implement incremental training. In "predict" action,
   * we can load model from file to generate predict results.
   */
  public static final String ANGEL_LOAD_MODEL_PATH = "angel.load.model.path";

  /** Application deploy mode, now support YARN and LOCAL mode */
  public static final String ANGEL_DEPLOY_MODE = "angel.deploy.mode";
  public static final String DEFAULT_ANGEL_DEPLOY_MODE = "YARN"; // YARN, LOCAL

  /**
   * Application running mode, now support ANGEL_PS_WORKER and ANGEL_PS.
   * <p>
   * ANGEL_PS_WORKER means startup workers and pss.
   * <p>
   * ANGEL_PS means only startup pss, tasks are executed by a third-party computing system.
   */
  public static final String ANGEL_RUNNING_MODE = ANGEL_PREFIX + "running.mode";
  public static final String DEFAULT_ANGEL_RUNNING_MODE = RunningMode.ANGEL_PS_WORKER.toString();

  /** User application jar package */
  public static final String ANGEL_JOB_JAR = ANGEL_PREFIX + "job.jar";

  /**
   * The lib jars used by the Angel application. If the Angel application want to use some third
   * part jars, it can add the paths of them to the parameter. The paths are separated by commas.
   */
  public static final String ANGEL_JOB_LIBJARS = ANGEL_PREFIX + "job.libjars";

  /**
   * The resource pool of application, it is used by YARN to allocate resources for the application.
   */
  public static final String ANGEL_QUEUE = "queue";

  /** Angel application name. */
  public static final String ANGEL_JOB_NAME = ANGEL_PREFIX + "job.name";
  public static final String DEFAULT_ANGEL_JOB_NAME = "angel app";

  /** Weather delete the output directory if it exists. */
  public static final String ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST = ANGEL_PREFIX
      + "output.path.deleteonexist";
  public static final boolean DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST = false;

  /** Weather delete the stage directory when Angel application exit. */
  public static final String ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE = ANGEL_PREFIX
      + "remove.staging.dir.enable";
  public static final boolean DEFAULT_ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE = true;

  /**
   * Angel application configuration file. Angel supports submitting a application with a
   * configuration file, which contains the parameters of the application.
   */
  public static final String ANGEL_APP_CONFIG_FILE = "angel.app.config.file";

  /** Angel application submit class. */
  public static final String ANGEL_APP_SUBMIT_CLASS = "angel.app.submit.class";
  public static final String DEFAULT_ANGEL_APP_SUBMIT_CLASS = DefaultAppSubmitter.class.getName();

  /** Use to upload files and archives used by the Angel application while submit the application. */
  public static final String ANGEL_JOB_CACHE_ARCHIVES = ANGEL_PREFIX + "job.cache.archives";
  public static final String ANGEL_JOB_CACHE_FILES = ANGEL_PREFIX + "job.cache.files";

  /** Application view ACL, it is used by YARN. */
  public static final String JOB_ACL_VIEW_JOB = ANGEL_PREFIX + "job.acl-view-job";
  public static final String DEFAULT_JOB_ACL_VIEW_JOB = " ";

  /** Application modify ACL, it is used by YARN. */
  public static final String JOB_ACL_MODIFY_JOB = ANGEL_PREFIX + "job.acl-modify-job";
  public static final String DEFAULT_JOB_ACL_MODIFY_JOB = ANGEL_PREFIX + "job.acl-modify-job";

  /** The completion cancel token for the application, it is used for YARN. */
  public static final String JOB_CANCEL_DELEGATION_TOKEN = ANGEL_PREFIX
      + "job.complete.cancel.delegation.tokens";

  /** The hostname of machine that sumbits the application. */
  public static final String JOB_SUBMITHOST = ANGEL_PREFIX + "job.submit.host";

  /** The ip of machine that submits the application. */
  public static final String JOB_SUBMITHOSTADDR = ANGEL_PREFIX + "job.submit.host.address";

  /** Angel application staging directory. */
  public static final String ANGEL_STAGING_DIR = ANGEL_PREFIX + "staging.dir";
  public static final String DEFAULT_ANGEL_STAGING_DIR = "/tmp/hadoop-yarn/";

  /** The name of user that submits the application. */
  public static final String USER_NAME = ANGEL_PREFIX + "submit.user.name";

  /** Angel application directory, it used to stored libjars and resource files. */
  public static final String ANGEL_JOB_DIR = ANGEL_PREFIX + "job.dir";

  /**
   * If the application need some resource files, it can use add the file lists to this parameter.
   * The files separated by commas.
   */
  public static final String ANGEL_APP_USER_RESOURCE_FILES = "angel.app.user.resource.files";

  /** If the application run over, it write final application state to this file, which can be used */
  public static final String ANGEL_APP_SERILIZE_STATE_FILE = "angel.app.serilize.state.file";

  /** Angel application output directory, this parameter is used by Angel itself. */
  public static final String ANGEL_JOB_OUTPUT_PATH = ANGEL_PREFIX + "output.path";

  /** Angel application temporary result output directory, this parameter is used by Angel itself. */
  public static final String ANGEL_JOB_TMP_OUTPUT_PATH_PREFIX= ANGEL_PREFIX + "tmp.output.path.prefix";

  /** Angel application temporary result output directory, this parameter is used by Angel itself. */
  public static final String ANGEL_JOB_TMP_OUTPUT_PATH = ANGEL_PREFIX + "tmp.output.path";

  /** The listen port range for all modules:AppMaster, Workers and PSs */
  public static final String ANGEL_LISTEN_PORT_RANGE = ANGEL_PREFIX + "listen.port.range";
  public static final String DEFAULT_ANGEL_LISTEN_PORT_RANGE = "20000,30000";

  /** Angel application type name, it is used the display name on YARN RM web page. */
  public static final String ANGEL_APPLICATION_TYPE = "ANGEL";

  /** Angel application id. */
  public static final String ANGEL_JOB_ID = ANGEL_PREFIX + "jobid";

  /** Angel application configuration file. */
  public static final String ANGEL_JOB_CONF_FILE = "job.xml";

  /** Local worker directory */
  public static final String LOCAL_DIR = "angel.cluster.local.dir";

  public static final String ANGEL_CLIENT_HEARTBEAT_INTERVAL_MS = "angel.client.heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_MS = 5000;

  public static final String ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS = "angel.client.heartbeat.interval.timeout.ms";
  public static final int DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS = 30000;

  // //////////////////////////////
  // Master Configs
  // //////////////////////////////

  /** Memory quota for AppMaster in MB. */
  @Deprecated
  public static final String ANGEL_AM_MEMORY_MB = ANGEL_AM_PREFIX + "memory.mb";
  @Deprecated
  public static final int DEFAULT_ANGEL_AM_MEMORY_MB = 2048;

  /** Memory quota for AppMaster in GB. */
  public static final String ANGEL_AM_MEMORY_GB = ANGEL_AM_PREFIX + "memory.gb";
  public static final int DEFAULT_ANGEL_AM_MEMORY_GB = 2;


  /** JVM parameters for AppMaster. */
  public static final String ANGEL_AM_JAVA_OPTS = ANGEL_AM_PREFIX + "java.opts";
  public static final String DEFAULT_ANGEL_AM_JAVA_OPTS = "-Xmx1024m";

  /** CPU vcore quota for AppMaster. */
  public static final String ANGEL_AM_CPU_VCORES = ANGEL_AM_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_AM_CPU_VCORES = 1;

  /** If there is no training data, workers are also started, just for test. */
  public static final String ANGEL_AM_USE_DUMMY_DATASPLITER = ANGEL_AM_PREFIX
      + "use.dummy.dataspliter";
  public static final boolean DEFAULT_ANGEL_AM_USE_DUMMY_DATASPLITER = false;

  /** The maximum number of times AppMaster can try. */
  public static final String ANGEL_AM_MAX_ATTEMPTS = ANGEL_PREFIX + "am.max-attempts";
  public static final int DEFAULT_ANGEL_AM_MAX_ATTEMPTS = 2;

  /** AppMaster log level for log4j. */
  public static final String ANGEL_AM_LOG_LEVEL = ANGEL_AM_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_AM_LOG_LEVEL = "INFO";

  /** AppMaster main class. */
  public static final String ANGEL_AM_CLASS = ANGEL_AM_PREFIX + "class";
  public static final String DEFAULT_ANGEL_AM_CLASS = AngelApplicationMaster.class.getName();

  /** AppMaster environment variable settings used for system. */
  public static final String ANGEL_AM_ADMIN_USER_ENV = ANGEL_AM_PREFIX + "admin.user.env";
  public static final String DEFAULT_ANGEL_AM_ADMIN_USER_ENV = "";

  /**
   * AppMaster environment variable settings used for user, it will cover environment variable
   * settings in ANGEL_AM_ADMIN_USER_ENV.
   */
  public static final String ANGEL_AM_ENV = ANGEL_AM_PREFIX + "env";
  public static final String DEFAULT_ANGEL_AM_ENV = "";

  /** Maximum number of threads used in yarn containers launching. */
  public static final String ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT = ANGEL_AM_PREFIX
      + "containerlauncher.thread.count";
  public static final int DEFAULT_ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT = 24;

  /** The time interval in milliseconds of AppMaster heartbeat to YARN RM. */
  public static final String ANGEL_AM_HEARTBEAT_INTERVAL_MS = ANGEL_AM_PREFIX
      + "heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_AM_HEARTBEAT_INTERVAL_MS = 5000;

  /**
   * The longest allowed time for AppMaster is in the temporary state, it is used to check time out
   * of control commands from AngelClient.
   */
  public static final String ANGEL_AM_APPSTATE_TIMEOUT_MS = ANGEL_AM_PREFIX + "appstate.timeout.ms";
  public static final long DEFAULT_ANGEL_AM_APPSTATE_TIMEOUT_MS = 600000;

  /** The time interval in milliseconds of AppMaster writing application states to hdfs. */
  public static final String ANGEL_AM_WRITE_STATE_INTERVAL_MS = ANGEL_AM_PREFIX
      + "write.state.interval.ms";
  public static final int DEFAULT_ANGEL_AM_WRITE_STATE_INTERVAL_MS = 10000;

  /** Slow ps/worker check polices */
  public static final String ANGEL_AM_SLOW_CHECK_POLICES = ANGEL_AM_PREFIX + "slow.check.polices";
  public static final String DEFAULT_ANGEL_AM_SLOW_CHECK_POLICES = TaskCalPerfChecker.class.getName();

  /** Slow ps/worker check enable*/
  public static final String ANGEL_AM_SLOW_CHECK_ENABLE = ANGEL_AM_PREFIX + "slow.check.enable";
  public static final boolean DEFAULT_ANGEL_AM_SLOW_CHECK_ENABLE = false;

  /** Slow ps/worker check interval in milliseconds */
  public static final String ANGEL_AM_SLOW_CHECK_INTERVAL_MS = ANGEL_AM_PREFIX + "slow.check.interval.ms";
  public static final int DEFAULT_ANGEL_AM_SLOW_CHECK_INTERVAL_MS = 60000;

  /** Task slowest discount, if a task calculate rate is slow than average rate * discount, the worker
   * the task is running on will be  considered to be a slow worker */
  public static final String ANGEL_AM_TASK_SLOWEST_DISCOUNT = ANGEL_AM_PREFIX + "task.slowest.discount";
  public static final double DEFAULT_ANGEL_AM_TASK_SLOWEST_DISCOUNT = 0.7;

  /**
   * The worker pool size for HDFS operation in Master
   */
  public static final String ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE = ANGEL_AM_PREFIX + "matrix.diskio.worker.pool.size";
  public static final int DEFAULT_ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE = Math.max(8, (int)(Runtime.getRuntime().availableProcessors() * 0.25));

  // //////////////////////////////
  // Worker Configs
  // //////////////////////////////
  /** The number of workergroups. It may be adjusted as the training data splits. */
  public static final String ANGEL_WORKERGROUP_NUMBER = ANGEL_WORKERGROUP_PREFIX + "number";
  public static final int DEFAULT_ANGEL_WORKERGROUP_NUMBER = 1;

  /**
   * The number of workergrous that actually startup. Limited by data splitting, the number of data
   * splits may be not equals to the setting value. Once that happens, the number of workergroups
   * may be adjusted to an approximate value.
   */
  public static final String ANGEL_WORKERGROUP_ACTUAL_NUM = ANGEL_WORKERGROUP_PREFIX
      + "actual.number";

  /** The number of workers in a workergroup, now just support a worker in a workergroup. */
  public static final String ANGEL_WORKERGROUP_WORKER_NUMBER = ANGEL_WORKERGROUP_PREFIX
      + "worker.number";
  public static final int DEFAULT_ANGEL_WORKERGROUP_WORKER_NUMBER = 1;

  /** The number of the tasks run in a worker. */
  public static final String ANGEL_WORKER_TASK_NUMBER = ANGEL_WORKER_PREFIX + "task.number";
  public static final int DEFAULT_ANGEL_WORKER_TASK_NUMBER = 1;

  /** The memory quota for a single worker in MB. */
  @Deprecated
  public static final String ANGEL_WORKER_MEMORY_MB = ANGEL_WORKER_PREFIX + "memory.mb";
  @Deprecated
  public static final int DEFAULT_ANGEL_WORKER_MEMORY_MB = 4096;

  /** The memory quota for a single worker in GB. */
  public static final String ANGEL_WORKER_MEMORY_GB = ANGEL_WORKER_PREFIX + "memory.gb";
  public static final int DEFAULT_ANGEL_WORKER_MEMORY_GB = 4;

  /** The CPU vcore quota for a single worker in MB. */
  public static final String ANGEL_WORKER_CPU_VCORES = ANGEL_WORKER_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_WORKER_CPU_VCORES = 1;

  /*** Worker environment variable settings. */
  public static final String ANGEL_WORKER_ENV = ANGEL_WORKER_PREFIX + "env";
  public static final String DEFAULT_ANGEL_WORK_ENV = "";

  /** Worker log level for log4j. */
  public static final String ANGEL_WORKER_LOG_LEVEL = ANGEL_WORKER_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_WORKER_LOG_LEVEL = "INFO";

  /** Worker JVM parameters. */
  public static final String ANGEL_WORKER_JAVA_OPTS = ANGEL_WORKER_PREFIX + "java.opts";

  /** Worker main class. */
  public static final String ANGEL_WORKER_CLASS = ANGEL_WORKER_PREFIX + "class";
  public static final String DEFAULT_ANGEL_WORKER_CLASS = Worker.class.getName();

  /**
   * Worker resource priority, it use to YARN container allocation. The smaller the priority, the
   * higher the priority.
   */
  public static final String ANGEL_WORKER_PRIORITY = ANGEL_WORKER_PREFIX + "priority";
  public static final int DEFAULT_ANGEL_WORKER_PRIORITY = 20;

  /**
   * The maxinum staleness value between tasks, it used to consistency controlling between tasks. 0
   * means BSP, bigger then 0 means SSP, -1 means ASYNC.
   */
  public static final String ANGEL_STALENESS = ANGEL_PREFIX + "staleness";
  public static final int DEFAULT_ANGEL_STALENESS = 0;

  /** The time interval in milliseconds of worker heartbeats to AppMaster. */
  public static final String ANGEL_WORKER_HEARTBEAT_INTERVAL = ANGEL_WORKER_PREFIX
      + "heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL = 5000;

  /**
   * The maximum time in milliseconds for AppMaster waiting for heartbeats from workers. Once a
   * worker does not send heartbeat to AppMaster during setting time, it can be considered that the
   * worker has been down.
   */
  public static final String ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS = ANGEL_WORKER_PREFIX
      + "heartbeat.timeout.ms";
  public static final long DEFAULT_ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS = 600000;

  public static final String ANGEL_WORKERGROUP_FAILED_TOLERATE = ANGEL_WORKERGROUP_PREFIX
      + "failed.tolerate";
  public static final double DEFAULT_WORKERGROUP_FAILED_TOLERATE = 0.1;

  public static final String ANGEL_TASK_ERROR_TOLERATE = ANGEL_PREFIX + "task.error.tolerate";
  public static final double DEFAULT_ANGEL_TASK_ERROR_TOLERATE = 0.01;
  
  /** The maximum number of times AppMaster can try. */
  public static final String ANGEL_WORKER_MAX_ATTEMPTS = ANGEL_WORKER_PREFIX + "max-attempts";
  public static final int DEFAULT_WORKER_MAX_ATTEMPTS = 4;

  /** The workers number for matrix operations */
  public static final String ANGEL_WORKER_MATRIX_EXECUTORS_NUM = ANGEL_WORKER_PREFIX + "matrix.executors.num";
  public static final int DEFAULT_ANGEL_WORKER_MATRIX_EXECUTORS_NUM = 16;


  // //////////////////////////////
  // Task Configs
  // //////////////////////////////
  /**
   * The number of total tasks that actually startup. Limited by data splitting, the number of data
   * splits may be not equals to the setting value. Once that happens, the number of tasks will be
   * adjusted to the number of data splits.
   */
  public static final String ANGEL_TASK_ACTUAL_NUM = ANGEL_TASK_PREFIX + "actual.number";

  /** Task iteration number. */
  public static final String ANGEL_TASK_ITERATION_NUMBER = ANGEL_TASK_PREFIX + "iteration.number";
  public static final int DEFAULT_ANGEL_TASK_ITERATION_NUMBER = 100;

  /** The task class that the workers will run, it is set by users. */
  public static final String ANGEL_TASK_USER_TASKCLASS = ANGEL_TASK_PREFIX + "user.task.class";
  public static final String DEFAULT_ANGEL_TASK_USER_TASKCLASS = BaseTask.class.getName();

  /**
   * The storage level for data blocks. There are three level now:memory, memory_disk, disk. The
   * default mode is memory_disk.
   * <p>
   * memory:all data blocks are stored in memory, if the worker's memory is large enough, we can use
   * this level.
   * <p>
   * memory_disk:try to keep all data blocks in memory, if the memory does not fit, put the extra
   * part into the disk.
   * <p>
   * disk:all data blocks are stored in disk.
   */
  public static final String ANGEL_TASK_DATA_STORAGE_LEVEL = ANGEL_TASK_PREFIX
      + "data.storage.level";
  public static final String DEFAULT_ANGEL_TASK_DATA_STORAGE_LEVEL = "memory_disk";

  /** The read buffer size for reading data from disk. */
  public static final String ANGEL_TASK_DISK_READ_BUFFER_SIZE = ANGEL_TASK_PREFIX
      + "disk.read.buffer.size";
  public static final int DEFAULT_ANGEL_TASK_DISK_READ_BUFFER_SIZE = 4 * 1024 * 1024;

  /** The maximum allowed memory in MB used in memory_disk level storage for every task. */
  public static final String ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB = ANGEL_TASK_PREFIX
      + "memory.storage.max.mb";
  public static final int DEFAULT_ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB = 1000;

  /** The number of samples used to estimate average size. */
  public static final String ANGEL_TASK_ESTIMIZE_SAMPLE_NUMBER = ANGEL_TASK_PREFIX
      + "estimize.sample.number";
  public static final int DEFAULT_ANGEL_TASK_ESTIMIZE_SAMPLE_NUMBER = 100;

  /** The write buffer size for writing data to disk. */
  public static final String ANGEL_TASK_DISK_WRITE_BUFFER_SIZE = ANGEL_TASK_PREFIX
      + "writer.buffer.size";
  public static final int DEFAULT_ANGEL_TASK_DISK_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  /** The maximum size in MB of a disk file. */
  public static final String ANGEL_TASK_RECORD_FILE_MAXSIZE_MB = ANGEL_TASK_PREFIX
      + "record.file.maxsize.mb";
  public static final int DEFAULT_ANGEL_TASK_RECORD_FILE_MAXSIZE_MB = 1024;

  // //////////////////////////////
  // ParameterServer Configs
  // //////////////////////////////
  /** The number of ps. */
  public static final String ANGEL_PS_NUMBER = ANGEL_PS_PREFIX + "number";
  public static final int DEFAULT_ANGEL_PS_NUMBER = 1;

  /** The number of ps. */
  public static final String ANGEL_PS_HA_REPLICATION_NUMBER = ANGEL_PS_PREFIX + "ha.replication.number";
  public static final int DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER = 1;

  public static final String ANGEL_PS_HA_USE_EVENT_PUSH = ANGEL_PS_PREFIX + "ha.use.event.push";
  public static final boolean DEFAULT_ANGEL_PS_HA_USE_EVENT_PUSH = false;

  public static final String ANGEL_PS_HA_PUSH_SYNC = ANGEL_PS_PREFIX + "ha.push.sync";
  public static final boolean DEFAULT_ANGEL_PS_HA_PUSH_SYNC = true;

  public static final String ANGEL_PS_HA_PUSH_INTERVAL_MS = ANGEL_PS_PREFIX + "push.interval.ms";
  public static final int DEFAULT_ANGEL_PS_HA_PUSH_INTERVAL_MS = 30000;

  /** The CPU vcore quota for a single ps. */
  public static final String ANGEL_PS_CPU_VCORES = ANGEL_PS_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_PS_CPU_VCORES = 1;

  /** The memory quota for a single worker in MB. */
  @Deprecated
  public static final String ANGEL_PS_MEMORY_MB = ANGEL_PS_PREFIX + "memory.mb";
  @Deprecated
  public static final int DEFAULT_ANGEL_PS_MEMORY_MB = 4096;

  /** The memory quota for a single worker in GB. */
  public static final String ANGEL_PS_MEMORY_GB = ANGEL_PS_PREFIX + "memory.gb";
  public static final int DEFAULT_ANGEL_PS_MEMORY_GB = 4;

  /** The time interval in milliseconds of a ps writing the snapshot for matrices to hdfs. */
  public static final String ANGEL_PS_BACKUP_INTERVAL_MS = ANGEL_PS_PREFIX + "backup.interval.ms";
  public static final int DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS = 300000;

  /** The matrices that need to backup in SnapshotDumper */
  public static final String ANGEL_PS_BACKUP_MATRICES = ANGEL_PS_PREFIX + "backup.matrices";

  /** The maximum number of times a ps can retry when run failed. */
  public static final String ANGEL_PS_MAX_ATTEMPTS = ANGEL_PS_PREFIX + "max-attempts";
  public static final int DEFAULT_PS_MAX_ATTEMPTS = 4;

  /** Ps environment variable settings. */
  public static final String ANGEL_PS_ENV = ANGEL_PS_PREFIX + "env";
  public static final String DEFAULT_ANGEL_PS_ENV = "";

  /** Ps log level for log4j. */
  public static final String ANGEL_PS_LOG_LEVEL = ANGEL_PS_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_PS_LOG_LEVEL = "INFO";

  /** Ps JVM parameters. */
  public static final String ANGEL_PS_JAVA_OPTS = ANGEL_PS_PREFIX + "child.opts";

  /** Ps main class. */
  public static final String ANGEL_PS_CLASS = ANGEL_PS_PREFIX + "class";
  public static final String DEFAULT_ANGEL_PS_CLASS = ParameterServer.class.getName();

  /** The time interval in milliseconds of ps heartbeats to AppMaster. */
  public static final String ANGEL_PS_HEARTBEAT_INTERVAL_MS = ANGEL_PS_PREFIX
      + "heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS = 5000;

  /** PS HA update sync worker number */
  public static final String ANGEL_PS_HA_SYNC_WORKER_NUM = ANGEL_PS_PREFIX + "ha.sync.worker.number";
  public static final int DEFAULT_ANGEL_PS_HA_SYNC_WORKER_NUM = Math.max(8, (int)(Runtime.getRuntime().availableProcessors() * 0.25));

  /** PS HA update sync worker send buffer size*/
  public static final String ANGEL_PS_HA_SYNC_SEND_BUFFER_SIZE = ANGEL_PS_PREFIX + "ha.sync.send.buffer.size";
  public static final int DEFAULT_ANGEL_PS_HA_SYNC_SEND_BUFFER_SIZE = 1024 * 1024;

  /**
   * Ps resource priority, it use to YARN container allocation. The smaller the priority, the higher
   * the priority.
   */
  public static final String ANGEL_PS_PRIORITY = ANGEL_PS_PREFIX + "priority";
  public static final int DEFAULT_ANGEL_PS_PRIORITY = 10;

  /**
   * The maximum time in milliseconds for AppMaster waiting for heartbeats from pss. Once a ps does
   * not send heartbeat to AppMaster during setting time, it can be considered that the ps has been
   * down.
   */
  public static final String ANGEL_PS_HEARTBEAT_TIMEOUT_MS = ANGEL_PS_PREFIX
      + "heartbeat.timeout.ms";
  public static final long DEFAULT_ANGEL_PS_HEARTBEAT_TIMEOUT_MS = 600000;

  /** The number of worker threads for commiting matrices in ps. */
  public static final String ANGEL_PS_COMMIT_TASK_NUM = ANGEL_PS_PREFIX + "commit.task.number";
  public static final int DEFAULT_ANGEL_PS_COMMIT_TASK_NUM = 5;

  /** The row updater class, the row update method can be defined by user. */
  public static final String ANGEL_PS_ROW_UPDATER_CLASS = ANGEL_PS_PREFIX + "row.updater.class";
  public static final Class<?> DEFAULT_ANGEL_PS_ROW_UPDATER = DefaultRowUpdater.class;

  /** PS executors thread pool size */
  public static final String ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE = ANGEL_PS_PREFIX +
    "matrix.diskio.worker.pool.size";

  /** Default PS executors thread pool size */
  public static final int DEFAULT_ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE = Math.max(16, (int)(Runtime.getRuntime().availableProcessors() * 0.25));

  public static final String ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE = ANGEL_PS_PREFIX +
    "max.partition.number.single.file";
  public static final int DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE = 100;


  // ////////////////// IPC //////////////////////////
  /** The read buffer size for rpc message encoded by protobuf. */
  public static final String ANGEL_REQUEST_PB_READBUFFER_LIMIT = "request.pb.readbuffer.limit";
  public static final int DEFAULT_ANGEL_REQUEST_PB_READBUFFER_LIMIT = 64 << 20;

  /** The maximum package size for a rpc message. */
  public static final String ANGEL_RPC_MAX_PACKAGE_SIZE = "angel.rpc.package.max.size";
  public static final int DEFAULT_ANGEL_RPC_MAX_PACKAGE_SIZE = 4 << 20;

  /** The maximum time in seconds waiting for a rpc response. */
  public static final String ANGEL_READ_TIMEOUT_SEC = "angel.read.timeout.sec";
  public static final int DEFAULT_ANGEL_READ_TIMEOUT_SEC = 100;

  /** The rpc retry time interval in milliseconds. */
  public static final String ANGEL_REQUEST_SLEEP_TIME_MS = "angel.request.sleep.time.ms";
  public static final int DEFAULT_ANGEL_REQUEST_SLEEP_TIME_MS = 1000;

  // //////////////////////////////
  // Matrix transfer Configs.
  // //////////////////////////////

  /** The maximum message size in a matrix transfer rpc. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE =
      "angel.netty.matrixtransfer.max.message.size";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE = 100 * 1024 * 1024;;

  /** The eventgroup thread number for netty client for matrix transfer. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM =
      "angel.netty.matrixtransfer.client.eventgroup.threadnum";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM = Runtime.getRuntime().availableProcessors() * 2;

  /** The send buffer size for netty client for matrix transfer. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF =
      "angel.netty.matrixtransfer.client.sndbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF = 1024 * 1024;

  /** The receive buffer size for netty client for matrix transfer. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF =
      "angel.netty.matrixtransfer.client.rcvbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF = 1024 * 1024;

  /** The eventgroup thread number for netty server for matrix transfer. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM =
      "angel.netty.matrixtransfer.server.eventgroup.threadnum";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM = Runtime.getRuntime().availableProcessors() * 2;


  /** The eventgroup thread number for netty server for serving transfer. */
  public static final String ANGEL_NETTY_SERVING_TRANSFER_SERVER_EVENTGROUP_THREADNUM =
      "angel.netty.serving.transfer.server.eventgroup.threadnum";
  public static final int DEFAULT_ANGEL_NETTY_SERVING_TRANSFER_SERVER_EVENTGROUP_THREADNUM = Runtime.getRuntime().availableProcessors() * 2;

  /** The send buffer size for netty server for matrix transfer. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF =
      "angel.netty.matrixtransfer.server.sndbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF = 1024 * 1024;

  /** The receive buffer size for netty server for matrix transfer. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF =
      "angel.netty.matrixtransfer.server.rcvbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF = 1024 * 1024;

  /**
   * The maximum allowed number of matrix transfer requests which are sending to a single
   * server(ps). It used to flow-control between psagent and ps.
   */
  public static final String ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER = ANGEL_PREFIX
      + "matrixtransfer.max.requestnum.perserver";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER = 8;

  public static final String ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE = ANGEL_PREFIX
    + "matrixtransfer.client.requester.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE = Math.max(16, (int)(Runtime.getRuntime().availableProcessors() * 0.5));

  public static final String ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE = ANGEL_PREFIX
    + "matrixtransfer.client.responser.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE = Math.max(16, (int)(Runtime.getRuntime().availableProcessors() * 0.5));


  public static final String ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE = ANGEL_PREFIX
    + "matrixtransfer.server.worker.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors();

  public static final String ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS = ANGEL_PREFIX
    + "matrixtransfer.server.token.timeout.ms";

  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS = 10000;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR = ANGEL_PREFIX
    + "matrixtransfer.server.rpc.limit.factor";
  public static final float DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR = 64.0f;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR = ANGEL_PREFIX
    + "matrixtransfer.server.rpc.limit.general.factor";
  public static float DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR = 0.0f;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE = ANGEL_PREFIX
    + "matrixtransfer.server.sender.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE = Math.max(8, (int)(Runtime.getRuntime().availableProcessors() * 0.25));

  public static final String ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER = ANGEL_PREFIX
    + "matrixtransfer.server.user.sender";
  public static final boolean DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER = false;

  public static final String ANGEL_MATRIX_OPLOG_MERGER_POOL_SIZE = ANGEL_PREFIX
    + "matrix.oplog.merger.pool.size";

  public static final int DEFAULT_ANGEL_MATRIX_OPLOG_MERGER_POOL_SIZE = Math.max(8, (int)(Runtime.getRuntime().availableProcessors() * 0.25));

  /**
   * The maximum allowed number of matrix transfer requests which are sending to the servers(ps). It
   * used to flow-control between psagent and ps.
   */
  public static final String ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM = ANGEL_PREFIX
      + "matrixtransfer.max.requestnum";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_MAX = 1024;

  /**
   * The maximum allowed size of requests/responses which are in flight. It used to flow-control
   * between psagent and ps.
   */
  public static final String ANGEL_NETWORK_MAX_BYTES_FLIGHT = "angel.network.max.bytes.flight";
  public static final int DEFAULT_ANGEL_NETWORK_MAX_BYTES_FLIGHT = 1000 * 1024 * 1024;

  /**
   * If the requests to a ps continuous failure over limit, the ps may be down, we will re-fetch the
   * location for the ps from AppMaster.
   */
  public static final String ANGEL_REFRESH_SERVERLOCATION_THRESHOLD = ANGEL_PREFIX
      + "refresh.serverlocation.threshold";
  public static final int DEFAULT_ANGEL_REFRESH_SERVERLOCATION_THRESHOLD = 5;

  /** The time interval in milliseconds for failed matrix transfer requests. */
  public static final String ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS = ANGEL_PREFIX
      + "matrixtransfer.retry.interval.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS = 10000;

  /** Weather we need use direct buffer in netty client. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER =
      "angel.netty.matrixtransfer.client.usedirectbuffer";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER = true;

  /** Weather we need use pooled buffer in netty server. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL =
    "angel.netty.matrixtransfer.client.usepool";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL = false;

  /** Weather we need use direct buffer in netty server. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER =
      "angel.netty.matrixtransfer.server.usedirectbuffer";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER = true;

  /** Weather we need use direct buffer in netty server. */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL =
    "angel.netty.matrixtransfer.server.usepool";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL = false;

  /**
   * The maximum time in milliseconds of waiting for the response. If the response of a request is
   * not received in allowed time, the request will be timeout. The timeout request will be retry
   * later.
   */
  public static final String ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS = ANGEL_PREFIX
      + "matrixtransfer.request.timeout.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS = 30000;

  /**
   * The time interval in milliseconds of clock events. We will check timeout requests and retry
   * failed requests when clock event happened.
   */
  public static final String ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS = ANGEL_PREFIX
      + "matrixtransfer.check.interval.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS = 100;

  // //////////////////////////////
  // Matrix transfer Configs.
  // //////////////////////////////
  public static final String ANGEL_PSAGENT_PREFIX = "angel.psagent.";
  /**
   * PSAgent caches synchronization time interval in milliseconds. The caches contain matrix caches
   * and clock caches, which synchronize data from pss regularly. Matrix caches are used to cache
   * matrix data splits. Clock caches are used to cache the clock values of all matrix partitions.
   * */
  public static final String ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS = ANGEL_PSAGENT_PREFIX
      + "cache.sync.timeinterval.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS = 5000;

  /** The matrix caches synchronization policy */
  public static final String ANGEL_PSAGENT_CACHE_SYNC_POLICY_CLASS = ANGEL_PSAGENT_PREFIX
      + "sync.policy.class";
  public static final String DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_POLICY_CLASS = DefaultPolicy.class
      .getName();

  public static final String ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS = ANGEL_PSAGENT_PREFIX
      + "to.ps.heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS = 5000;

  public static final String ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS = ANGEL_PSAGENT_PREFIX
      + "to.ps.heartbeat.timeout.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS = 20000;

  /**
   * The machine addresses on which the pss are expected to run. The addressed are separated by
   * commas.
   */
  public static final String ANGEL_PS_IP_LIST = ANGEL_PS_PREFIX + "ip.list";

  /** Weather we need update the matrix clock and task iteration to master synchronously. */
  public static final String ANGEL_PSAGENT_SYNC_CLOCK_ENABLE = ANGEL_PSAGENT_PREFIX
      + "sync.clock.enable";
  public static final boolean DEFAULT_ANGEL_PSAGENT_SYNC_CLOCK_ENABLE = true;

  // Configs used to ANGEL_PS_PSAGENT running mode future.
  public static final String ANGEL_PSAGENT_NUMBER = ANGEL_PSAGENT_PREFIX + "number";
  public static final int DEFAULT_ANGEL_PSAGENT_NUMBER = 1;
  public static final String ANGEL_PSAGENT_MERMORY_MB = ANGEL_PSAGENT_PREFIX + "memory.mb";
  public static final int DEFAULT_ANGEL_PSAGENT_MERMORY_MB = 20000;
  public static final String ANGEL_PSAGENT_CPU_VCORES = ANGEL_PSAGENT_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_PSAGENT_CPU_VCORES = 5;
  public static final String ANGEL_PSAGENT_PRIORITY = ANGEL_PSAGENT_PREFIX + "priority";
  public static final int DEFAULT_ANGEL_PSAGENT_PRIORITY = 5;
  public static final String ANGEL_PSAGENT_MAX_ATTEMPTS = ANGEL_PSAGENT_PREFIX + "max.attempts";
  public static final int DEFAULT_ANGEL_PSAGENT_MAX_ATTEMPTS = 4;
  public static final String ANGEL_PSAGENT_ENV = ANGEL_PSAGENT_PREFIX + "env";
  public static final String DEFAULT_ANGEL_PSAGENT_ENV = "";
  public static final String ANGEL_PSAGNET_LOG_LEVEL = ANGEL_PSAGENT_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_PSAGNET_LOG_LEVEL = "INFO";
  public static final String ANGEL_PSAGENT_JAVA_OPTS = ANGEL_PSAGENT_PREFIX + "java.opts";
  public static final String ANGEL_PSAGENT_CLASS = ANGEL_PSAGENT_PREFIX + "class";
  public static final String DEFAULT_ANGEL_PSAGENT_CLASS = PSAgent.class.getName();
  public static final String ANGEL_PSAGENT_IPLIST = ANGEL_PSAGENT_PREFIX + "iplist";
  public static final String ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS = ANGEL_PSAGENT_PREFIX
      + "heartbeat.timeout.ms";
  public static final long DEFAULT_ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS = 60000;

  // model parse
  public static final String ANGEL_MODEL_PARSE_THREAD_COUNT = "angel.model.parse.thread.count";
  public static final String ANGEL_MODEL_PARSE_NAME = "angel.model.parse.name";
  public static final String ANGEL_PARSE_MODEL_PATH = "angel.parse.model.path";
  public static final int DEFAULT_ANGEL_MODEL_PARSE_THREAD_COUNT = 5;


  public static final String ML_CONNECTION_TIMEOUT_MILLIS = "ml.connection.timeout";
  /** If not specified, the default connection timeout will be used (3 sec). */
  public static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 3 * 1000L;
  public static final String CONNECTION_READ_TIMEOUT_SEC = "ml.connection.read.timeout";
  /**
   * If not specified, the default connection read timeout will be used (300 sec).
   */
  public static final int DEFAULT_CONNECTION_READ_TIMEOUT_SEC = 60 * 5;
  public static final int DEFAULT_READ_TIMEOUT_SEC = 10;
  public static final String SERVER_IO_THREAD = "netty.server.io.threads";
  public static final String NETWORK_IO_MODE = "netty.io.mode";

  public static final String CLIENT_IO_THREAD = "netty.client.io.threads";
  /**
   * timeout for each RPC
   */
  public static final String ML_RPC_TIMEOUT_KEY = "ml.rpc.timeout";
  public static final int DEFAULT_ML_RPC_TIMEOUT = 60000;
  public static final String ML_CLIENT_RPC_MAXATTEMPTS = "ml.client.rpc.maxattempts";
  
  // Mark whether use pyangel or not.
  public static final String ANGEL_API_TYPE = "angel.app.type";
  
  public static final String PYANGEL_PYTHON = "angel.pyangel.python";
  
  public static final String PYANGEL_PYFILE = "angel.pyangel.pyfile";
  
  public static final String PYANGEL_PYDEPFILES = "angel.pyangel.pyfile.dependencies";

  public static final String ANGEL_PLUGIN_SERVICE_ENABLE =  "angel.plugin.service.enable";
  public static final String ANGEL_SERVING_SHARDING_NUM =  "angel.serving.sharding.num";
  public static final String ANGEL_SERVING_SHARDING_CONCURRENT_CAPACITY = "angel.serving.sharding.concurrent.capacity";
  public static final String ANGEL_SERVING_SHARDING_MODEL_CLASS= "angel.serving.sharding.model.class";
  public static final String ANGEL_SERVING_MASTER_IP= "angel.serving.master.ip";
  public static final String ANGEL_SERVING_MASTER_PORT= "angel.serving.master.port";
  public static final String ANGEL_SERVING_MODEL_NAME= "angel.serving.model.name";
  public static final String ANGEL_SERVING_MODEL_LOAD_TIMEOUT_MINUTE= "angel.serving.model.load.timeout.minute";
  public static final String ANGEL_SERVING_MODEL_LOAD_CHECK_INTEVAL_SECOND= "angel.serving.model.load.check.inteval.second";
  public static final String ANGEL_SERVING_MODEL_LOAD_TYPE= "angel.serving.model.load.type";
  public static final String ANGEL_SERVING_PREDICT_LOCAL_OUTPUT= "angel.serving.predict.local.output";

  /**
   * Default value of {@link #ML_CLIENT_RPC_MAXATTEMPTS}.
   */
  public static final int DEFAULT_ML_CLIENT_RPC_MAXATTEMPTS = 1;
  public static int[] RETRY_BACKOFF = {1, 1, 1, 2, 2, 4, 4, 8, 16, 32};


  public static Configuration create() {
    Configuration conf = new Configuration();
    conf.setClassLoader(AngelConf.class.getClassLoader());
    return addAngelResources(conf);
  }

  public static Configuration addAngelResources(Configuration conf) {
    conf.addResource(ANGEL_DEFAULT_XML_FILE);
    conf.addResource(ANGEL_SITE_XML_FILE);
    return conf;
  }

  public Properties getAngelProps() {
    return getProps();
  }

  /**
   * @param that Configuration to clone.
   * @return the cloned configuration
   */
  public static Configuration clone(final Configuration that) {
    Configuration conf = new Configuration(that);
    conf.setClassLoader(AngelConf.class.getClassLoader());
    return conf;
  }

  /**
   * Merge two configurations.
   * 
   * @param destConf the configuration that will be overwritten with items from the srcConf
   * @param srcConf the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Map.Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }
}
