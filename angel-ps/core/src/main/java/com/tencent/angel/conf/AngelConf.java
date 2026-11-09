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


package com.tencent.angel.conf;

import com.tencent.angel.RunningMode;
import com.tencent.angel.data.inputformat.BalanceInputFormat;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.slowcheck.TaskCalPerfChecker;
import com.tencent.angel.ps.ParameterServer;
import com.tencent.angel.ps.storage.matrix.PartitionSourceMap;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.utils.DefaultAppSubmitter;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.task.BaseTask;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

/**
 * Angel system parameters.
 */
public class AngelConf extends Configuration {

  public AngelConf(Configuration conf) {
    super(conf);
  }

  public AngelConf() {
    super();
  }

  private static final String ANGEL_DEFAULT_XML_FILE = "angel-default.xml";
  private static final String ANGEL_SITE_XML_FILE = "angel-site.xml";

  private static final String ANGEL_PREFIX = "angel.";
  private static final String ANGEL_AM_PREFIX = "angel.am.";
  private static final String ANGEL_WORKER_PREFIX = "angel.worker.";
  private static final String ANGEL_PS_PREFIX = "angel.ps.";
  private static final String ANGEL_TASK_PREFIX = "angel.task.";
  private static final String ANGEL_WORKERGROUP_PREFIX = "angel.workergroup.";
  public static final String ANGEL_KUBERNETES_PREFIX = "angel.kubernetes.";
  public static final String ANGEL_KUBERNETES_MASTER_LABEL_PREFIX = "angel.kubernetes.master.label.";
  public static final String ANGEL_KUBERNETES_MASTER_ANNOTATION_PREFIX = "angel.kubernetes.master.annotation.";
  public static final String ANGEL_KUBERNETES_MASTER_SECRETS_PREFIX = "angel.kubernetes.master.secrets.";
  public static final String ANGEL_KUBERNETES_MASTER_SECRET_KEY_REF_PREFIX = "angel.kubernetes.master.secretKeyRef.";
  public static final String ANGEL_KUBERNETES_MASTER_ENV_PREFIX = "angel.kubernetes.masterEnv.";
  public static final String ANGEL_KUBERNETES_EXECUTOR_ENV_PREFIX = "angel.kubernetes.executorEnv.";
  public static final String ANGEL_KUBERNETES_MASTER_VOLUMES_PREFIX = "angel.kubernetes.master.volumes.";
  public static final String ANGEL_KUBERNETES_EXECUTOR_LABEL_PREFIX = "angel.kubernetes.executor.label.";
  public static final String ANGEL_KUBERNETES_EXECUTOR_ANNOTATION_PREFIX = "angel.kubernetes.executor.annotation.";
  public static final String ANGEL_KUBERNETES_EXECUTOR_SECRETS_PREFIX = "angel.kubernetes.executor.secrets.";
  public static final String ANGEL_KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX = "angel.kubernetes.executor.secretKeyRef.";
  public static final String ANGEL_KUBERNETES_EXECUTOR_VOLUMES_PREFIX = "angel.kubernetes.executor.volumes.";
  public static final String ANGEL_KUBERNETES_VOLUMES_HOSTPATH_TYPE = "hostPath";
  public static final String ANGEL_KUBERNETES_VOLUMES_PVC_TYPE = "persistentVolumeClaim";
  public static final String ANGEL_KUBERNETES_VOLUMES_EMPTYDIR_TYPE = "emptyDir";
  public static final String ANGEL_KUBERNETES_VOLUMES_MOUNT_PATH_KEY = "mount.path";
  public static final String ANGEL_KUBERNETES_VOLUMES_MOUNT_READONLY_KEY = "mount.readOnly";
  public static final String ANGEL_KUBERNETES_VOLUMES_OPTIONS_PATH_KEY = "options.path";
  public static final String ANGEL_KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY = "options.claimName";
  public static final String ANGEL_KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY = "options.medium";
  public static final String ANGEL_KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY = "options.sizeLimit";

  // //////////////////////////////
  // Application Configs
  // //////////////////////////////
  /**
   * angel application id
   */
  public static final String ANGEL_APPLICATION_ID = "angel.application.id";

  /**
   * Task action type. There are two action types now:train and predict.
   * <p>
   * "train" action type means training model use training data.
   * <p>
   * "predict" action type means predict result use model.
   */
  public static final String ANGEL_ACTION_TYPE = "action.type";
  public static final String DEFAULT_ANGEL_ACTION_TYPE = "train";

  /**
   * Training data path.
   */
  public static final String ANGEL_TRAIN_DATA_PATH = "angel.train.data.path";
  public static final String ANGEL_VALIDATE_DATA_PATH = "angel.validate.data.path";

  /**
   * kerberos config
   */
  public static final String ANGEL_KERBEROS_KEYTAB = "angel.kerberos.keytab";
  public static final String ANGEL_KERBEROS_PRINCIPAL = "angel.kerberos.principal";
  public static final String ANGEL_KERBEROS_KEYTAB_NAME = "angel.kerberos.keytab.name";

  /**
   * Predict data path.
   */
  public static final String ANGEL_PREDICT_DATA_PATH = "angel.predict.data.path";

  /**
   * Input data path use by Angel
   */
  public static final String ANGEL_JOB_INPUT_PATH = "angel.job.input.path";

  /**
   * Training data file format.
   */
  public static final String ANGEL_INPUTFORMAT_CLASS = ANGEL_PREFIX + "input.format";
  public static final String DEFAULT_ANGEL_INPUTFORMAT_CLASS = BalanceInputFormat.class.getName();

  /**
   * Predict result output path. If use "predict" action, we need set it.
   */
  public static final String ANGEL_PREDICT_PATH = "angel.predict.out.path";

  /**
   * Serving temp output path. If use "serving" action, we need set it.
   */
  public static final String ANGEL_SERVING_TEMP_PATH = "angel.serving.temp.path";

  /**
   * Serving client type. If use "serving" action, we need set it.
   */
  public static final String ANGEL_SERVING_CLIENT_TYPE = "angel.serving.client.type";

  /**
   * Model save path. This parameter is used in "train" action.
   */
  public static final String ANGEL_SAVE_MODEL_PATH = "angel.save.model.path";

  /**
   * Enable epoch trigger model save
   */
  public static final String ANGEL_SAVE_MODEL_EPOCH_TIGGER_ENABLE =
      "angel.save.model.epoch.trigger.enable";
  public static final boolean DEFAULT_ANGEL_SAVE_MODEL_EPOCH_TIGGER_ENABLE = false;

  /**
   * Maximum save results number
   */
  public static final String ANGEL_SAVE_MODEL_MAX_RESULTS_FOR_SINGLE_MATRIX =
      "angel.save.model.max.results.for.single.matrix";
  public static final int DEFAULT_ANGEL_SAVE_MODEL_MAX_RESULTS_FOR_SINGLE_MATRIX = 4;

  /**
   * Maximum save results number
   */
  public static final String ANGEL_CHECKPOINT_MAX_RESULTS_FOR_SINGLE_MATRIX =
      "angel.checkpoint.max.results.for.single.matrix";
  public static final int DEFAULT_ANGEL_CHECKPOINT_MAX_RESULTS_FOR_SINGLE_MATRIX = 1;

  /**
   * Save model every how many epochs, it is effective only as "angel.save.model.epoch.trigger.enable"
   * is set to true
   */
  public static final String ANGEL_SAVE_MODEL_EVERY_HOWMANY_EPOCHS =
      "angel.save.model.every.howmany.epochs";
  public static final int DEFAULT_ANGEL_SAVE_MODEL_EVERY_HOWMANY_EPOCHS = 1;

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

  /**
   * Application deploy mode, now support YARN and LOCAL mode
   */
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

  /**
   * User application jar package
   */
  public static final String ANGEL_JOB_JAR = ANGEL_PREFIX + "job.jar";
  public static final String ANGEL_ML_CONF = "angel.ml.conf";

  /**
   * The lib jars used by the Angel application. If the Angel application want to use some third
   * part jars, it can add the paths of them to the parameter. The paths are separated by commas.
   */
  public static final String ANGEL_JOB_LIBJARS = ANGEL_PREFIX + "job.libjars";

  /**
   * The resource pool of application, it is used by YARN to allocate resources for the
   * application.
   */
  public static final String ANGEL_QUEUE = "queue";

  /**
   * Angel application name.
   */
  public static final String ANGEL_JOB_NAME = ANGEL_PREFIX + "job.name";
  public static final String DEFAULT_ANGEL_JOB_NAME = "angel app";


  public static final String ANGEL_PLATFORM_USER = "angel.platform.user";
  public static final String DEFAULT_PLATFORM_USER = "tesla";

  /**
   * Weather delete the output directory if it exists.
   */
  public static final String ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST =
      ANGEL_PREFIX + "output.path.deleteonexist";
  public static final boolean DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST = false;

  /**
   * Weather delete the stage directory when Angel application exit.
   */
  public static final String ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE =
      ANGEL_PREFIX + "remove.staging.dir.enable";
  public static final boolean DEFAULT_ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE = true;

  /**
   * Angel application configuration file. Angel supports submitting a application with a
   * configuration file, which contains the parameters of the application.
   */
  public static final String ANGEL_APP_CONFIG_FILE = "angel.app.config.file";

  /**
   * Angel application submit class.
   */
  public static final String ANGEL_APP_SUBMIT_CLASS = "angel.app.submit.class";
  public static final String DEFAULT_ANGEL_APP_SUBMIT_CLASS = DefaultAppSubmitter.class.getName();

  /**
   * Use to upload files and archives used by the Angel application while submit the application.
   */
  public static final String ANGEL_JOB_CACHE_ARCHIVES = ANGEL_PREFIX + "job.cache.archives";
  public static final String ANGEL_JOB_CACHE_FILES = ANGEL_PREFIX + "job.cache.files";

  /**
   * Application view ACL, it is used by YARN.
   */
  public static final String JOB_ACL_VIEW_JOB = ANGEL_PREFIX + "job.acl-view-job";
  public static final String DEFAULT_JOB_ACL_VIEW_JOB = " ";

  /**
   * Application modify ACL, it is used by YARN.
   */
  public static final String JOB_ACL_MODIFY_JOB = ANGEL_PREFIX + "job.acl-modify-job";
  public static final String DEFAULT_JOB_ACL_MODIFY_JOB = ANGEL_PREFIX + "job.acl-modify-job";

  /**
   * The completion cancel token for the application, it is used for YARN.
   */
  public static final String JOB_CANCEL_DELEGATION_TOKEN =
      ANGEL_PREFIX + "job.complete.cancel.delegation.tokens";

  /**
   * The hostname of machine that sumbits the application.
   */
  public static final String JOB_SUBMITHOST = ANGEL_PREFIX + "job.submit.host";

  /**
   * The ip of machine that submits the application.
   */
  public static final String JOB_SUBMITHOSTADDR = ANGEL_PREFIX + "job.submit.host.address";

  /**
   * Angel application staging directory.
   */
  public static final String ANGEL_STAGING_DIR = ANGEL_PREFIX + "staging.dir";
  public static final String DEFAULT_ANGEL_STAGING_DIR = "/tmp/hadoop-yarn/";

  /**
   * The name of user that submits the application.
   */
  public static final String USER_NAME = ANGEL_PREFIX + "submit.user.name";

  /**
   * Angel application directory, it used to stored libjars and resource files.
   */
  public static final String ANGEL_JOB_DIR = ANGEL_PREFIX + "job.dir";

  /**
   * If the application need some resource files, it can use add the file lists to this parameter.
   * The files separated by commas.
   */
  public static final String ANGEL_APP_USER_RESOURCE_FILES = "angel.app.user.resource.files";

  /**
   * If the application run over, it write final application state to this file, which can be used
   */
  public static final String ANGEL_APP_SERILIZE_STATE_FILE = "angel.app.serilize.state.file";

  /**
   * Angel application output directory, this parameter is used by Angel itself.
   */
  public static final String ANGEL_JOB_OUTPUT_PATH = ANGEL_PREFIX + "output.path";

  /**
   * Angel application temporary result output directory, this parameter is used by Angel itself.
   */
  public static final String ANGEL_JOB_TMP_OUTPUT_PATH_PREFIX =
      ANGEL_PREFIX + "tmp.output.path.prefix";

  /**
   * The Spark staging directory path (e.g. hdfs://cluster/tmp/spark/.sparkStaging/{SPARK_APP_ID}).
   * Used to resolve bare filenames in libjars that reside in the Spark staging directory.
   */
  public static final String ANGEL_APP_SPARK_STAGING_DIR = "angel.app.spark.staging.dir";

  /**
   * Angel application temporary result output directory, this parameter is used by Angel itself.
   */
  public static final String ANGEL_JOB_TMP_OUTPUT_PATH = ANGEL_PREFIX + "tmp.output.path";

  /**
   * The listen port range for all modules:AppMaster, Workers and PSs
   */
  public static final String ANGEL_LISTEN_PORT_RANGE = ANGEL_PREFIX + "listen.port.range";
  public static final String DEFAULT_ANGEL_LISTEN_PORT_RANGE = "20000,30000";
  public static final String ANGEL_EMBEDDED_APP_ID = "angel.embedded.app.id";
  /**
   * Angel application type name, it is used the display name on YARN RM web page.
   */
  public static final String ANGEL_APPLICATION_TYPE = "ANGEL";

  /**
   * Angel application id.
   */
  public static final String ANGEL_JOB_ID = ANGEL_PREFIX + "jobid";

  /**
   * Angel application configuration file.
   */
  public static final String ANGEL_JOB_CONF_FILE = "job.xml";

  /**
   * Local worker directory
   */
  public static final String LOCAL_DIR = "angel.cluster.local.dir";

  public static final String ANGEL_CLIENT_HEARTBEAT_INTERVAL_MS =
      "angel.client.heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_MS = 5000;

  public static final String ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS =
      "angel.client.heartbeat.interval.timeout.ms";
  public static final int DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS = 600000;

  // //////////////////////////////
  // Master Configs
  // //////////////////////////////

  /**
   * Memory quota for AppMaster in MB.
   */
  @Deprecated
  public static final String ANGEL_AM_MEMORY_MB = ANGEL_AM_PREFIX + "memory.mb";
  @Deprecated
  public static final int DEFAULT_ANGEL_AM_MEMORY_MB = 4096;

  /**
   * Memory quota for AppMaster in GB.
   */
  public static final String ANGEL_AM_MEMORY_GB = ANGEL_AM_PREFIX + "memory.gb";
  public static final int DEFAULT_ANGEL_AM_MEMORY_GB = 4;


  /**
   * JVM parameters for AppMaster.
   */
  public static final String ANGEL_AM_JAVA_OPTS = ANGEL_AM_PREFIX + "java.opts";
  public static final String DEFAULT_ANGEL_AM_JAVA_OPTS = "-Xmx1024m";

  /**
   * Whether {@link #ANGEL_AM_JAVA_OPTS} should be <em>appended</em> to the auto-generated
   * default JVM parameters instead of fully replacing them.
   *
   * <p>Historical behaviour ({@code false}, the default): if the user sets
   * {@code angel.am.java.opts}, the entire {@code masterJVMOptions} block in
   * {@code AngelYarnClient} is skipped. That means heap sizing (-Xmx/-Xms derived from
   * {@code angel.am.memory.gb}), GC logging (-Xloggc:&lt;LOG_DIR&gt;/gc.log,
   * +PrintGCDateStamps, +PrintGCDetails, +PrintCommandLineFlags, +PrintTenuringDistribution,
   * +PrintAdaptiveSizePolicy) are all silently dropped unless the user re-states every one
   * of them. The most common production fallout is operators losing the GC log and being
   * unable to diagnose AM exit-137 post-hoc.
   *
   * <p>When set to {@code true}, the framework first generates the full default master JVM
   * parameter string and then appends the user-supplied opts after it. Because the JVM
   * honours the <em>last</em> occurrence of a given flag, this lets users override individual
   * flags (e.g. just bump {@code -Xmx} or add {@code -XX:+UseG1GC}) without losing the rest
   * of the defaults, including the GC log path.
   */
  public static final String ANGEL_AM_JAVA_OPTS_APPEND_MODE =
      ANGEL_AM_PREFIX + "java.opts.append.mode";
  public static final boolean DEFAULT_ANGEL_AM_JAVA_OPTS_APPEND_MODE = false;

  /**
   * CPU vcore quota for AppMaster.
   */
  public static final String ANGEL_AM_CPU_VCORES = ANGEL_AM_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_AM_CPU_VCORES = 1;

  /**
   * If there is no training data, workers are also started, just for test.
   */
  public static final String ANGEL_AM_USE_DUMMY_DATASPLITER =
      ANGEL_AM_PREFIX + "use.dummy.dataspliter";
  public static final boolean DEFAULT_ANGEL_AM_USE_DUMMY_DATASPLITER = false;

  /**
   * The maximum number of times AppMaster can try.
   */
  public static final String ANGEL_AM_MAX_ATTEMPTS = ANGEL_PREFIX + "am.max-attempts";
  public static final int DEFAULT_ANGEL_AM_MAX_ATTEMPTS = 2;

  /**
   * AppMaster log level for log4j.
   */
  public static final String ANGEL_AM_LOG_LEVEL = ANGEL_AM_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_AM_LOG_LEVEL = "INFO";

  /**
   * AppMaster main class.
   */
  public static final String ANGEL_AM_CLASS = ANGEL_AM_PREFIX + "class";
  public static final String DEFAULT_ANGEL_AM_CLASS = AngelApplicationMaster.class.getName();

  /**
   * AppMaster environment variable settings used for system.
   */
  public static final String ANGEL_AM_ADMIN_USER_ENV = ANGEL_AM_PREFIX + "admin.user.env";
  public static final String DEFAULT_ANGEL_AM_ADMIN_USER_ENV = "";

  /**
   * AppMaster environment variable settings used for user, it will cover environment variable
   * settings in ANGEL_AM_ADMIN_USER_ENV.
   */
  public static final String ANGEL_AM_ENV = ANGEL_AM_PREFIX + "env";
  public static final String DEFAULT_ANGEL_AM_ENV = "";

  /**
   * Maximum number of threads used in yarn containers launching.
   */
  public static final String ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT =
      ANGEL_AM_PREFIX + "containerlauncher.thread.count";
  public static final int DEFAULT_ANGEL_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT = 24;

  /**
   * The time interval in milliseconds of AppMaster heartbeat to YARN RM.
   */
  public static final String ANGEL_AM_HEARTBEAT_INTERVAL_MS =
      ANGEL_AM_PREFIX + "heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_AM_HEARTBEAT_INTERVAL_MS = 5000;

  /**
   * The longest allowed time for AppMaster is in the temporary state, it is used to check time out
   * of control commands from AngelClient.
   */
  public static final String ANGEL_AM_APPSTATE_TIMEOUT_MS = ANGEL_AM_PREFIX + "appstate.timeout.ms";
  public static final long DEFAULT_ANGEL_AM_APPSTATE_TIMEOUT_MS = 3000000;

  /**
   * The time interval in milliseconds of AppMaster writing application states to hdfs.
   */
  public static final String ANGEL_AM_WRITE_STATE_INTERVAL_MS =
      ANGEL_AM_PREFIX + "write.state.interval.ms";
  public static final int DEFAULT_ANGEL_AM_WRITE_STATE_INTERVAL_MS = 10000;

  /**
   * Slow ps/worker check polices
   */
  public static final String ANGEL_AM_SLOW_CHECK_POLICES = ANGEL_AM_PREFIX + "slow.check.polices";
  public static final String DEFAULT_ANGEL_AM_SLOW_CHECK_POLICES =
      TaskCalPerfChecker.class.getName();

  /**
   * Slow ps/worker check enable
   */
  public static final String ANGEL_AM_SLOW_CHECK_ENABLE = ANGEL_AM_PREFIX + "slow.check.enable";
  public static final boolean DEFAULT_ANGEL_AM_SLOW_CHECK_ENABLE = false;

  /**
   * Slow ps/worker check interval in milliseconds
   */
  public static final String ANGEL_AM_SLOW_CHECK_INTERVAL_MS =
      ANGEL_AM_PREFIX + "slow.check.interval.ms";
  public static final int DEFAULT_ANGEL_AM_SLOW_CHECK_INTERVAL_MS = 60000;

  /**
   * Task slowest discount, if a task calculate rate is slow than average rate * discount, the
   * worker the task is running on will be  considered to be a slow worker
   */
  public static final String ANGEL_AM_TASK_SLOWEST_DISCOUNT =
      ANGEL_AM_PREFIX + "task.slowest.discount";
  public static final double DEFAULT_ANGEL_AM_TASK_SLOWEST_DISCOUNT = 0.7;

  /**
   * The worker pool size for HDFS operation in Master
   */
  public static final String ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE =
      ANGEL_AM_PREFIX + "matrix.diskio.worker.pool.size";
  public static final int DEFAULT_ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE =
      Math.max(8, (int) (Runtime.getRuntime().availableProcessors() * 0.25));

  public static final String ANGEL_MODEL_PARTITIONER_PARTITION_SIZE =
      "angel.model.partitioner.partition.size";
  public static final long DEFAULT_ANGEL_MODEL_PARTITIONER_PARTITION_SIZE = 500000;

  public static final String ANGEL_MODEL_PARTITIONER_MAX_PARTITION_NUM =
      "angel.model.partitioner.max.partition.number";
  public static final int DEFAULT_ANGEL_MODEL_PARTITIONER_MAX_PARTITION_NUM = 500;

  public static final String ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER =
      "angel.model.partitioner.partition.number.perserver";
  public static final int DEFAULT_ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER = 5;

  public static final String ANGEL_AM_HOST = ANGEL_AM_PREFIX + "host";
  public static final String ANGEL_AM_PORT = ANGEL_AM_PREFIX + "port";

  // //////////////////////////////
  // Worker Configs
  // //////////////////////////////
  /**
   * The number of workergroups. It may be adjusted as the training data splits.
   */
  public static final String ANGEL_WORKERGROUP_NUMBER = ANGEL_WORKERGROUP_PREFIX + "number";
  public static final int DEFAULT_ANGEL_WORKERGROUP_NUMBER = 1;

  /**
   * The number of workergrous that actually startup. Limited by data splitting, the number of data
   * splits may be not equals to the setting value. Once that happens, the number of workergroups
   * may be adjusted to an approximate value.
   */
  public static final String ANGEL_WORKERGROUP_ACTUAL_NUM =
      ANGEL_WORKERGROUP_PREFIX + "actual.number";

  /**
   * The number of workers in a workergroup, now just support a worker in a workergroup.
   */
  public static final String ANGEL_WORKERGROUP_WORKER_NUMBER =
      ANGEL_WORKERGROUP_PREFIX + "worker.number";
  public static final int DEFAULT_ANGEL_WORKERGROUP_WORKER_NUMBER = 1;

  /**
   * The number of the tasks run in a worker.
   */
  public static final String ANGEL_WORKER_TASK_NUMBER = ANGEL_WORKER_PREFIX + "task.number";
  public static final int DEFAULT_ANGEL_WORKER_TASK_NUMBER = 1;

  /**
   * The memory quota for a single worker in MB.
   */
  @Deprecated
  public static final String ANGEL_WORKER_MEMORY_MB = ANGEL_WORKER_PREFIX + "memory.mb";
  @Deprecated
  public static final int DEFAULT_ANGEL_WORKER_MEMORY_MB = 4096;

  /**
   * The memory quota for a single worker in GB.
   */
  public static final String ANGEL_WORKER_MEMORY_GB = ANGEL_WORKER_PREFIX + "memory.gb";
  public static final int DEFAULT_ANGEL_WORKER_MEMORY_GB = 4;

  /**
   * The CPU vcore quota for a single worker in MB.
   */
  public static final String ANGEL_WORKER_CPU_VCORES = ANGEL_WORKER_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_WORKER_CPU_VCORES = 1;

  /*** Worker environment variable settings. */
  public static final String ANGEL_WORKER_ENV = ANGEL_WORKER_PREFIX + "env";
  public static final String DEFAULT_ANGEL_WORK_ENV = "";

  /**
   * Worker log level for log4j.
   */
  public static final String ANGEL_WORKER_LOG_LEVEL = ANGEL_WORKER_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_WORKER_LOG_LEVEL = "INFO";

  /**
   * Worker JVM parameters.
   */
  public static final String ANGEL_WORKER_JAVA_OPTS = ANGEL_WORKER_PREFIX + "java.opts";

  /**
   * Whether {@link #ANGEL_WORKER_JAVA_OPTS} should be <em>appended</em> to the auto-generated
   * default JVM parameters instead of fully replacing them.
   *
   * <p>Historical behaviour ({@code false}, the default): if the user sets
   * {@code angel.worker.java.opts}, the entire {@code generateDefaultJVMParameters} block is
   * skipped. That means heap sizing (-Xmx), direct memory limit, GC algorithm selection and
   * GC logging flags are all silently dropped unless the user re-states every one of them.
   *
   * <p>When set to {@code true}, the framework first generates the full default JVM parameter
   * string (driven by {@code angel.worker.memory.gb}, GC tuning knobs, etc.) and then appends
   * the user-supplied opts after it. Because JVM honours the <em>last</em> occurrence of a
   * given flag, this lets users override individual flags (e.g. just bump {@code -Xmx} or
   * switch GC) without losing the rest of the defaults — including {@code -Xloggc:...} which
   * is essential for post-mortem GC analysis on container-OOM (exit 137) events.
   */
  public static final String ANGEL_WORKER_JAVA_OPTS_APPEND_MODE =
      ANGEL_WORKER_PREFIX + "java.opts.append.mode";
  public static final boolean DEFAULT_ANGEL_WORKER_JAVA_OPTS_APPEND_MODE = false;

  /**
   * Worker main class.
   */
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

  /**
   * The time interval in milliseconds of worker heartbeats to AppMaster.
   */
  public static final String ANGEL_WORKER_HEARTBEAT_INTERVAL_MS =
      ANGEL_WORKER_PREFIX + "heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_WORKER_HEARTBEAT_INTERVAL = 5000;

  /**
   * The maximum time in milliseconds for AppMaster waiting for heartbeats from workers. Once a
   * worker does not send heartbeat to AppMaster during setting time, it can be considered that the
   * worker has been down.
   */
  public static final String ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS =
      ANGEL_WORKER_PREFIX + "heartbeat.timeout.ms";
  public static final long DEFAULT_ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS = 600000;

  public static final String ANGEL_WORKERGROUP_FAILED_TOLERATE =
      ANGEL_WORKERGROUP_PREFIX + "failed.tolerate";
  public static final double DEFAULT_WORKERGROUP_FAILED_TOLERATE = 0.1;

  public static final String ANGEL_TASK_ERROR_TOLERATE = ANGEL_PREFIX + "task.error.tolerate";
  public static final double DEFAULT_ANGEL_TASK_ERROR_TOLERATE = 0.0;

  /**
   * The maximum number of times AppMaster can try.
   */
  public static final String ANGEL_WORKER_MAX_ATTEMPTS = ANGEL_WORKER_PREFIX + "max-attempts";
  public static final int DEFAULT_WORKER_MAX_ATTEMPTS = 4;

  public static final String ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF =
      ANGEL_WORKER_PREFIX + "jvm.direct.factor.use.direct.buff";
  public static final float DEFAULT_ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF = 0.15f;

  public static final String ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_HEAP_BUFF =
      ANGEL_WORKER_PREFIX + "jvm.direct.factor.use.heap.buff";
  public static final float DEFAULT_ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_HEAP_BUFF = 0.1f;

  public static final String ANGEL_WORKER_JVM_YOUNG_FACTOR =
      ANGEL_WORKER_PREFIX + "jvm.young.factor";
  public static final float DEFAULT_ANGEL_WORKER_JVM_YOUNG_FACTOR = 0.15f;

  /**
   * Enable G1 in Worker
   */
  public static final String ANGEL_WORKER_JVM_USE_G1 = ANGEL_WORKER_PREFIX + "jvm.use.g1";
  public static final boolean DEFAULT_ANGEL_WORKER_JVM_USE_G1 = true;

  /**
   * Max GC pause time
   */
  public static final String ANGEL_WORKER_JVM_G1_MAXPAUSETIME_MS =
      ANGEL_WORKER_PREFIX + "jvm.g1.maxpausetime.ms";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_MAXPAUSETIME_MS = 2000;

  /**
   * Minimal young region ratio
   */
  public static final String ANGEL_WORKER_JVM_G1_MIN_NEWRATIO =
      ANGEL_WORKER_PREFIX + "jvm.g1.min.newratio";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_MIN_NEWRATIO = 5;

  /**
   * Maximal young region ratio
   */
  public static final String ANGEL_WORKER_JVM_G1_MAX_NEWRATIO =
      ANGEL_WORKER_PREFIX + "jvm.g1.max.newratio";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_MAX_NEWRATIO = 60;

  /**
   * Region size
   */
  public static final String ANGEL_WORKER_JVM_G1_REGIONSIZE_MB =
      ANGEL_WORKER_PREFIX + "jvm.g1.regionsize.mb";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_REGIONSIZE_MB = 32;

  /**
   * InitiatingHeapOccupancyPercent
   */
  public static final String ANGEL_WORKER_JVM_G1_IHOP = ANGEL_WORKER_PREFIX + "jvm.g1.ihop";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_IHOP = 60;

  /**
   * G1MixedGCLiveThresholdPercent
   */
  public static final String ANGEL_WORKER_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT =
      ANGEL_WORKER_PREFIX + "jvm.g1.mixgc.live.threshold.percent";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT = 65;

  /**
   * G1MixedGCCountTarget
   */
  public static final String ANGEL_WORKER_JVM_G1_MIXGC_TARGET_COUNT =
      ANGEL_WORKER_PREFIX + "jvm.g1.mixgc.target.count";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_MIXGC_TARGET_COUNT = 8;

  /**
   * ParallelGCThreads
   */
  public static final String ANGEL_WORKER_JVM_G1_WORKER_NUM =
      ANGEL_WORKER_PREFIX + "jvm.g1.worker.num";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_WORKER_NUM = 4;

  /**
   * ConcGCThreads
   */
  public static final String ANGEL_WORKER_JVM_G1_CONC_WORKER_NUM =
      ANGEL_WORKER_PREFIX + "jvm.g1.conc.worker.num";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_CONC_WORKER_NUM = 4;

  /**
   * G1ReservePercent
   */
  public static final String ANGEL_WORKER_JVM_G1_RESERVE_PERCENT =
      ANGEL_WORKER_PREFIX + "jvm.g1.reserve.percent";
  public static final int DEFAULT_ANGEL_WORKER_JVM_G1_RESERVE_PERCENT = 10;

  /** Worker counterpart of {@link #ANGEL_PS_JVM_G1_PIN_HEAP_SIZE}; same rationale. */
  public static final String ANGEL_WORKER_JVM_G1_PIN_HEAP_SIZE =
      ANGEL_WORKER_PREFIX + "jvm.g1.pin.heap.size";
  public static final boolean DEFAULT_ANGEL_WORKER_JVM_G1_PIN_HEAP_SIZE = true;

  /** Worker counterpart of {@link #ANGEL_PS_JVM_G1_ALWAYS_PRE_TOUCH}; same rationale. */
  public static final String ANGEL_WORKER_JVM_G1_ALWAYS_PRE_TOUCH =
      ANGEL_WORKER_PREFIX + "jvm.g1.always.pre.touch";
  public static final boolean DEFAULT_ANGEL_WORKER_JVM_G1_ALWAYS_PRE_TOUCH = true;

  /** Worker counterpart of {@link #ANGEL_PS_JVM_G1_PARALLEL_REF_PROC}; same rationale. */
  public static final String ANGEL_WORKER_JVM_G1_PARALLEL_REF_PROC =
      ANGEL_WORKER_PREFIX + "jvm.g1.parallel.ref.proc";
  public static final boolean DEFAULT_ANGEL_WORKER_JVM_G1_PARALLEL_REF_PROC = true;

  /**
   * Reserved-for-non-heap memory inside the worker container, in MB. The worker JVM's
   * total RSS is approximately {@code -Xmx + -XX:MaxDirectMemorySize + this overhead}, where
   * "this overhead" covers Metaspace, code cache, thread stacks, native libraries (libhdfs,
   * netty epoll, libtcmalloc, etc.), JVM-internal data structures, and direct-buffer
   * headroom that is not pre-reserved.
   *
   * <p>Historically the code subtracted a fixed 512MB which is far too small for a modern
   * deployment running netty + hadoop native + spark + various ML kernels — the resulting
   * RSS routinely overshoots the container quota and the NodeManager SIGKILLs the JVM
   * (visible as {@code Container killed on request. Exit code is 137}). Default raised to
   * 1024MB and made configurable so operators can grow it on heavyweight deployments
   * without touching code. Set to a value strictly less than the container memory.
   */
  public static final String ANGEL_WORKER_JVM_OVERHEAD_MB =
      ANGEL_WORKER_PREFIX + "jvm.overhead.mb";
  public static final int DEFAULT_ANGEL_WORKER_JVM_OVERHEAD_MB = 1024;

  /**
   * The workers number for matrix operations
   */
  public static final String ANGEL_WORKER_MATRIX_EXECUTORS_NUM =
      ANGEL_WORKER_PREFIX + "matrix.executors.num";
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

  /**
   * Task iteration number.
   */
  public static final String ANGEL_TASK_ITERATION_NUMBER = ANGEL_TASK_PREFIX + "iteration.number";
  public static final int DEFAULT_ANGEL_TASK_ITERATION_NUMBER = 100;

  /**
   * The task class that the workers will run, it is set by users.
   */
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
  public static final String ANGEL_TASK_DATA_STORAGE_LEVEL =
      ANGEL_TASK_PREFIX + "data.storage.level";
  public static final String DEFAULT_ANGEL_TASK_DATA_STORAGE_LEVEL = "memory_disk";

  /**
   * The read buffer size for reading data from disk.
   */
  public static final String ANGEL_TASK_DISK_READ_BUFFER_SIZE =
      ANGEL_TASK_PREFIX + "disk.read.buffer.size";
  public static final int DEFAULT_ANGEL_TASK_DISK_READ_BUFFER_SIZE = 4 * 1024 * 1024;

  /**
   * The maximum allowed memory in MB used in memory_disk level storage for every task.
   */
  public static final String ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB =
      ANGEL_TASK_PREFIX + "memory.storage.max.mb";
  public static final int DEFAULT_ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB = 1000;

  /**
   * The number of samples used to estimate average size.
   */
  public static final String ANGEL_TASK_ESTIMIZE_SAMPLE_NUMBER =
      ANGEL_TASK_PREFIX + "estimize.sample.number";
  public static final int DEFAULT_ANGEL_TASK_ESTIMIZE_SAMPLE_NUMBER = 100;

  /**
   * The write buffer size for writing data to disk.
   */
  public static final String ANGEL_TASK_DISK_WRITE_BUFFER_SIZE =
      ANGEL_TASK_PREFIX + "writer.buffer.size";
  public static final int DEFAULT_ANGEL_TASK_DISK_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  /**
   * The maximum size in MB of a disk file.
   */
  public static final String ANGEL_TASK_RECORD_FILE_MAXSIZE_MB =
      ANGEL_TASK_PREFIX + "record.file.maxsize.mb";
  public static final int DEFAULT_ANGEL_TASK_RECORD_FILE_MAXSIZE_MB = 1024;

  // //////////////////////////////
  // ParameterServer Configs
  // //////////////////////////////
  /**
   * The number of ps.
   */
  public static final String ANGEL_PS_NUMBER = ANGEL_PS_PREFIX + "number";
  public static final int DEFAULT_ANGEL_PS_NUMBER = 1;

  /**
   * The number of ps.
   */
  public static final String ANGEL_PS_HA_REPLICATION_NUMBER =
      ANGEL_PS_PREFIX + "ha.replication.number";
  public static final int DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER = 1;

  public static final String ANGEL_PS_HA_USE_EVENT_PUSH = ANGEL_PS_PREFIX + "ha.use.event.push";
  public static final boolean DEFAULT_ANGEL_PS_HA_USE_EVENT_PUSH = false;

  public static final String ANGEL_PS_HA_PUSH_SYNC = ANGEL_PS_PREFIX + "ha.push.sync";
  public static final boolean DEFAULT_ANGEL_PS_HA_PUSH_SYNC = true;

  public static final String ANGEL_PS_HA_PUSH_INTERVAL_MS = ANGEL_PS_PREFIX + "push.interval.ms";
  public static final int DEFAULT_ANGEL_PS_HA_PUSH_INTERVAL_MS = 30000;

  /**
   * The CPU vcore quota for a single ps.
   */
  public static final String ANGEL_PS_CPU_VCORES = ANGEL_PS_PREFIX + "cpu.vcores";
  public static final int DEFAULT_ANGEL_PS_CPU_VCORES = 1;

  /**
   * The memory quota for a single worker in MB.
   */
  @Deprecated
  public static final String ANGEL_PS_MEMORY_MB = ANGEL_PS_PREFIX + "memory.mb";
  @Deprecated
  public static final int DEFAULT_ANGEL_PS_MEMORY_MB = 4096;

  /**
   * The memory quota for a single worker in GB.
   */
  public static final String ANGEL_PS_MEMORY_GB = ANGEL_PS_PREFIX + "memory.gb";
  public static final int DEFAULT_ANGEL_PS_MEMORY_GB = 4;

  public static final String ANGEL_PS_BACKUP_AUTO_ENABLE = ANGEL_PS_PREFIX + "backup.auto.enable";
  public static final boolean DEFAULT_ANGEL_PS_BACKUP_AUTO_ENABLE = true;

  /**
   * The time interval in milliseconds of a ps writing the snapshot for matrices to hdfs.
   */
  public static final String ANGEL_PS_BACKUP_INTERVAL_MS = ANGEL_PS_PREFIX + "backup.interval.ms";
  public static final int DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS = 300000;

  public static final String ANGEL_PS_BACKUP_PARALLEL = ANGEL_PS_PREFIX + "backup.parallel";
  public static final int DEFAULT_ANGEL_PS_BACKUP_PARALLEL = 16;

  /**
   * The matrices that need to backup in SnapshotDumper
   */
  public static final String ANGEL_PS_BACKUP_MATRICES = ANGEL_PS_PREFIX + "backup.matrices";

  /**
   * The maximum number of times a ps can retry when run failed.
   */
  public static final String ANGEL_PS_MAX_ATTEMPTS = ANGEL_PS_PREFIX + "max-attempts";
  public static final int DEFAULT_PS_MAX_ATTEMPTS = 4;

  /**
   * Ps environment variable settings.
   */
  public static final String ANGEL_PS_ENV = ANGEL_PS_PREFIX + "env";
  public static final String DEFAULT_ANGEL_PS_ENV = "";

  /**
   * Ps log level for log4j.
   */
  public static final String ANGEL_PS_LOG_LEVEL = ANGEL_PS_PREFIX + "log.level";
  public static final String DEFAULT_ANGEL_PS_LOG_LEVEL = "INFO";

  /**
   * Ps JVM parameters.
   */
  public static final String ANGEL_PS_JAVA_OPTS = ANGEL_PS_PREFIX + "java.opts";

  /**
   * Whether {@link #ANGEL_PS_JAVA_OPTS} (or the legacy {@code angel.ps.child.opts}) should be
   * <em>appended</em> to the auto-generated default JVM parameters instead of fully replacing
   * them.
   *
   * <p>Historical behaviour ({@code false}, the default): if the user sets
   * {@code angel.ps.java.opts}, the entire {@code generateDefaultJVMParameters} block is
   * skipped. That means heap sizing (-Xmx), {@code -XX:MaxDirectMemorySize}, GC algorithm
   * selection, the G1 STW-pause hardening flags (+AlwaysPreTouch, pinned -Xms,
   * +ParallelRefProcEnabled) and GC logging are all silently dropped unless the user
   * re-states every one of them. Operators routinely forget {@code -Xloggc:...}, leaving
   * exit-137 container-OOM events undiagnosable post-hoc.
   *
   * <p>When set to {@code true}, the framework first generates the full default JVM parameter
   * string (driven by {@code angel.ps.memory.gb}, {@code angel.ps.jvm.overhead.mb} and the
   * GC tuning knobs) and then appends the user-supplied opts after it. Because the JVM
   * honours the <em>last</em> occurrence of a given flag, this lets users override individual
   * flags (e.g. just bump {@code -Xmx} or switch GC) without losing the rest of the defaults.
   */
  public static final String ANGEL_PS_JAVA_OPTS_APPEND_MODE =
      ANGEL_PS_PREFIX + "java.opts.append.mode";
  public static final boolean DEFAULT_ANGEL_PS_JAVA_OPTS_APPEND_MODE = false;

  /**
   * STANDALONE deploy mode: classpath used for child PS / Worker JVMs.
   * Defaults to the current process's java.class.path.
   */
  public static final String ANGEL_STANDALONE_CLASSPATH = "angel.standalone.classpath";

  /**
   * STANDALONE deploy mode: directory to store stdout/stderr logs of child PS / Worker JVMs.
   * Defaults to ${java.io.tmpdir}/angel-standalone-logs.
   */
  public static final String ANGEL_STANDALONE_LOG_DIR = "angel.standalone.log.dir";

  /**
   * STANDALONE deploy mode: directory under which the master materialises the runtime
   * {@code job.xml} that every PS / Worker child JVM picks up via classpath. Defaults
   * to {@code ${angel.standalone.log.dir}/<appId>-conf}, which is fine for single-node
   * setups but useless when other machines (e.g. external PSes started by hand) need
   * to read the same {@code job.xml}.
   *
   * <p>Set this to a path on a shared filesystem (NFS, CephFS, JuiceFS, ...) that all
   * PS hosts can read, so a manually-launched PS can simply point
   * {@code angel-ps-launch --job-conf <dir>/job.xml} at the same file. The directory
   * is created on demand if it does not exist; when this property is set explicitly
   * the master will <strong>not</strong> delete it on shutdown (multiple clusters may
   * share the same staging area).
   *
   * <p>Mainly useful together with
   * {@link #ANGEL_STANDALONE_PS_EXTERNAL}={@code true}.
   */
  public static final String ANGEL_STANDALONE_JOB_CONF_DIR =
      "angel.standalone.job.conf.dir";



  // ---- STANDALONE distributed (multi-host via SSH) options --------------------------------

  /**
   * Comma-separated list of hosts that PS / Worker processes can be scheduled to in
   * STANDALONE mode. If unset, all containers are launched on the local machine (single-node
   * standalone). Each host may be {@code host} or {@code host:port}; port overrides
   * {@link #ANGEL_STANDALONE_SSH_PORT}.
   */
  public static final String ANGEL_STANDALONE_HOSTS = "angel.standalone.hosts";

  /** Optional, overrides {@link #ANGEL_STANDALONE_HOSTS} for PS containers only. */
  public static final String ANGEL_STANDALONE_PS_HOSTS = "angel.standalone.ps.hosts";

  /** Optional, overrides {@link #ANGEL_STANDALONE_HOSTS} for Worker containers only. */
  public static final String ANGEL_STANDALONE_WORKER_HOSTS = "angel.standalone.worker.hosts";

  /** SSH user used to log in to remote hosts. Defaults to {@code ${user.name}}. */
  public static final String ANGEL_STANDALONE_SSH_USER = "angel.standalone.ssh.user";

  /** Default SSH port for remote hosts (overridable per-host with {@code host:port}). */
  public static final String ANGEL_STANDALONE_SSH_PORT = "angel.standalone.ssh.port";
  public static final int DEFAULT_ANGEL_STANDALONE_SSH_PORT = 22;

  /**
   * Path of the {@code ssh} client executable on the submitting machine. Defaults to "ssh".
   */
  public static final String ANGEL_STANDALONE_SSH_CMD = "angel.standalone.ssh.cmd";
  public static final String DEFAULT_ANGEL_STANDALONE_SSH_CMD = "ssh";

  /**
   * Path of the {@code scp} client executable on the submitting machine. Defaults to "scp".
   */
  public static final String ANGEL_STANDALONE_SCP_CMD = "angel.standalone.scp.cmd";
  public static final String DEFAULT_ANGEL_STANDALONE_SCP_CMD = "scp";

  /**
   * Extra options passed to {@code ssh}/{@code scp}, e.g. {@code "-i /path/key -o StrictHostKeyChecking=no"}.
   */
  public static final String ANGEL_STANDALONE_SSH_OPTS = "angel.standalone.ssh.opts";

  /**
   * Path of the {@code java} executable on remote hosts (relative or absolute). Defaults to "java".
   */
  public static final String ANGEL_STANDALONE_REMOTE_JAVA = "angel.standalone.remote.java";
  public static final String DEFAULT_ANGEL_STANDALONE_REMOTE_JAVA = "java";

  /**
   * Classpath used by remote PS / Worker JVMs. Required when {@link #ANGEL_STANDALONE_HOSTS}
   * is set to non-local hosts: jars must be pre-deployed at this path on every remote node.
   * Glob patterns like {@code /opt/angel/lib/*} are supported by the JVM.
   */
  public static final String ANGEL_STANDALONE_REMOTE_CLASSPATH =
      "angel.standalone.remote.classpath";

  /**
   * Working directory created on each remote host for this application. The submitter scp's
   * the materialized job.xml into this directory and child JVMs prepend it to their classpath
   * so that {@code conf.addResource("job.xml")} works.
   * Defaults to {@code /tmp/angel/<appId>}.
   */
  public static final String ANGEL_STANDALONE_REMOTE_WORKDIR = "angel.standalone.remote.workdir";

  /**
   * Directory on remote hosts where stdout/stderr of PS / Worker JVMs is appended.
   * Defaults to {@code /tmp/angel-standalone-logs}.
   */
  public static final String ANGEL_STANDALONE_REMOTE_LOG_DIR =
      "angel.standalone.remote.log.dir";
  public static final String DEFAULT_ANGEL_STANDALONE_REMOTE_LOG_DIR =
      "/tmp/angel-standalone-logs";

  /**
   * Optional per-role override for where {@link com.tencent.angel.AngelDeployMode#STANDALONE}
   * PS / Worker child processes write their stdout/stderr file. When unset, we fall back
   * to {@link #ANGEL_STANDALONE_REMOTE_LOG_DIR} for remote launches and
   * {@link #ANGEL_STANDALONE_LOG_DIR} for local launches. Use these when you want to
   * separate PS / Worker logs into different directories (e.g. mounting a faster disk
   * for PS hot logs).
   *
   * <p>The launcher creates the directory on the target host (remote case) or locally
   * before redirecting output into it.
   */
  public static final String ANGEL_STANDALONE_PS_LOG_DIR =
      "angel.standalone.ps.log.dir";
  public static final String ANGEL_STANDALONE_WORKER_LOG_DIR =
      "angel.standalone.worker.log.dir";

  /**
   * Path to a log4j (1.x or 2.x) configuration file the STANDALONE PS / Worker child
   * JVMs should use. The launcher injects it as both
   * {@code -Dlog4j.configuration=file:<path>} (log4j 1.x) and
   * {@code -Dlog4j.configurationFile=<path>} (log4j 2.x) so the child picks up the
   * right one regardless of which binding wins on its classpath.
   *
   * <p>The file path is resolved on the host where the child runs: for remote PSes use
   * a path that exists on those machines (typically deployed alongside the Angel jars).
   *
   * <p>If unset, no log4j system property is injected and the child falls back to
   * whatever {@code log4j.properties} / {@code log4j2.xml} happens to be on its
   * classpath. The default Angel distribution ships
   * {@code $ANGEL_HOME/conf/log4j.properties} which works once {@code conf/} is on
   * the classpath.
   */
  public static final String ANGEL_STANDALONE_PS_LOG4J_FILE =
      "angel.standalone.ps.log4j.file";
  public static final String ANGEL_STANDALONE_WORKER_LOG4J_FILE =
      "angel.standalone.worker.log4j.file";

  /**
   * Convenience root-logger level for the STANDALONE PS / Worker child JVMs (e.g.
   * {@code INFO}, {@code DEBUG}, {@code WARN}). When set the launcher injects
   * {@code -Dangel.ps.log.level=<level>} (resp. {@code .worker.log.level}) so a
   * suitably-written log4j config can pick it up via
   * {@code log4j.rootLogger=${angel.ps.log.level}, ...}. Has no effect on its own
   * unless your log4j config references this variable.
   */
  public static final String ANGEL_STANDALONE_PS_LOG_LEVEL =
      "angel.standalone.ps.log.level";
  public static final String ANGEL_STANDALONE_WORKER_LOG_LEVEL =
      "angel.standalone.worker.log.level";


  /**
   * When set to {@code true} the master runs in <i>external-PS</i> mode: it allocates
   * PSAttempts and registers them in the heartbeat monitor as usual, but does NOT spawn
   * any child JVMs through {@code StandaloneContainerLauncher}. Operators are expected
   * to start each PS process by hand on whichever machine they choose, using the
   * {@code bin/angel-ps-launch} helper (or any equivalent invocation of
   * {@link com.tencent.angel.ps.StandalonePSMain}).
   *
   * <p>This is the right setting when the parameter servers are pre-provisioned (e.g.
   * baked into a container image, started by an external orchestrator, or attached to a
   * GPU / RDMA host that needs custom JVM flags). It removes the master's hard
   * dependency on passwordless ssh and on a static {@code angel.standalone.ps.hosts}
   * list.
   *
   * <p>The master writes a discovery manifest right after the cluster is up so each PS
   * host knows which {@code PARAMETERSERVER_ID / PS_ATTEMPT_ID} to claim. See
   * {@link #ANGEL_STANDALONE_PS_MANIFEST_FILE}.
   *
   * <p>Only honored in {@link com.tencent.angel.AngelDeployMode#STANDALONE}. Defaults
   * to {@code false}.
   */
  public static final String ANGEL_STANDALONE_PS_EXTERNAL =
      "angel.standalone.ps.external";
  public static final boolean DEFAULT_ANGEL_STANDALONE_PS_EXTERNAL = false;

  /**
   * Driver-side opt-out for {@code AngelPSContext.waitPS()} when the spark application
   * is attached to a pre-launched Angel cluster (typically via
   * {@code spark.angel.ps.master=<host:port>}).
   *
   * <p>{@link com.tencent.angel.client.AngelClient#waitForAllPS(int)} compares the
   * driver-supplied {@code angel.ps.number} against the master-tracked PS count using a
   * STRICT equality check, so any mismatch (e.g. the driver defaulting to 1 while the
   * cluster runs 2 PSes) hangs the driver indefinitely inside
   * {@code HGNN.initialize -> waitPS -> waitForAllPS}.</p>
   *
   * <p>The recommended cluster bootstrap (see {@code bin/examples/start-angel-spark-cluster.sh})
   * already gates spark-submit on every PS having logged {@code "Register to AppMaster
   * successfully"}, making the driver-side wait redundant. Set this conf to {@code true}
   * (e.g. {@code --conf spark.hadoop.angel.ps.attach.skip.wait=true}) to skip the wait
   * altogether and rely on the cluster bootstrap as the single source of readiness.</p>
   *
   * <p>Has no effect when the driver itself launches Angel (yarn / local /
   * embedded-standalone modes).</p>
   */
  public static final String ANGEL_PS_ATTACH_SKIP_WAIT =
      "angel.ps.attach.skip.wait";
  public static final boolean DEFAULT_ANGEL_PS_ATTACH_SKIP_WAIT = false;

  /**
   * Path of the PS-launch manifest written by {@code AngelPSClusterMain} when the
   * cluster is up in {@link #ANGEL_STANDALONE_PS_EXTERNAL} mode. The file lists, one
   * line per PS attempt, the values an operator needs to bring up the PS by hand:
   * {@code <serverIndex> <attemptIndex> <masterIp> <masterPort> <jobXmlPath>}. When
   * unset the manifest is suppressed (the same data is still printed on stdout).
   */
  public static final String ANGEL_STANDALONE_PS_MANIFEST_FILE =
      "angel.standalone.ps.manifest.file";



  /**
   * Ps main class.
   */
  public static final String ANGEL_PS_CLASS = ANGEL_PS_PREFIX + "class";
  public static final String DEFAULT_ANGEL_PS_CLASS = ParameterServer.class.getName();

  /**
   * The time interval in milliseconds of ps heartbeats to AppMaster.
   */
  public static final String ANGEL_PS_HEARTBEAT_INTERVAL_MS =
      ANGEL_PS_PREFIX + "heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS = 5000;

  /**
   * Maximum number of <i>consecutive</i> heartbeat failures a PS will tolerate before
   * declaring the master lost and shutting itself down. Only honored in
   * {@link com.tencent.angel.AngelDeployMode#STANDALONE} (where there is no YARN RM to
   * restart the AM); for YARN deployments the existing recovery path is kept intact and
   * this setting has no effect.
   *
   * <p>Defaults to {@code 60}, which combined with the default 5 s heartbeat interval
   * means a PS exits after roughly 5 minutes of master unreachability.
   *
   * <p>Set to a non-positive value to disable the self-kill behaviour entirely.
   */
  public static final String ANGEL_PS_HEARTBEAT_FAILURE_TOLERATE_COUNT =
      ANGEL_PS_PREFIX + "heartbeat.failure.tolerate.count";
  public static final int DEFAULT_ANGEL_PS_HEARTBEAT_FAILURE_TOLERATE_COUNT = 60;

  /**
   * Explicit listen host that a PS advertises to the master during
   * {@code psRegister}. When unset, the PS falls back to
   * {@link com.tencent.angel.utils.NetUtils#getRealLocalIP()} which probes network
   * interfaces. Set this to the machine's real routable IP whenever
   * {@code /etc/hosts} on the PS host maps the hostname to a loopback address
   * (otherwise the master would receive "127.0.0.1:<port>" and other nodes could
   * not reach the PS). In STANDALONE distributed mode the launcher passes the
   * target host through automatically so users rarely need to set this by hand.
   */
  public static final String ANGEL_PS_LISTEN_HOST =
      ANGEL_PS_PREFIX + "listen.host";

  /**
   * Explicit listen host a Worker binds to and advertises. Same rationale as
   * {@link #ANGEL_PS_LISTEN_HOST} but for the Worker role.
   */
  public static final String ANGEL_WORKER_LISTEN_HOST =
      ANGEL_WORKER_PREFIX + "listen.host";

  /**
   * PS HA update sync worker number
   */
  public static final String ANGEL_PS_HA_SYNC_WORKER_NUM =
      ANGEL_PS_PREFIX + "ha.sync.worker.number";
  public static final int DEFAULT_ANGEL_PS_HA_SYNC_WORKER_NUM =
      Math.max(8, (int) (Runtime.getRuntime().availableProcessors() * 0.25));

  /**
   * PS HA update sync worker send buffer size
   */
  public static final String ANGEL_PS_HA_SYNC_SEND_BUFFER_SIZE =
      ANGEL_PS_PREFIX + "ha.sync.send.buffer.size";
  public static final int DEFAULT_ANGEL_PS_HA_SYNC_SEND_BUFFER_SIZE = 1024 * 1024;

  /**
   * File read/write buffer size in PS
   */
  public static final String ANGEL_PS_IO_FILE_BUFFER_SIZE = ANGEL_PS_PREFIX + "io.file.buffer.size";
  public static final int DEFAULT_ANGEL_PS_IO_FILE_BUFFER_SIZE = 4 * 1024 * 1024;

  /**
   * Ps resource priority, it use to YARN container allocation. The smaller the priority, the higher
   * the priority.
   */
  public static final String ANGEL_PS_PRIORITY = ANGEL_PS_PREFIX + "priority";
  public static final int DEFAULT_ANGEL_PS_PRIORITY = 10;

  public static final String ANGEL_PS_REQUEST_RESOURCE_USE_MINIBATCH = ANGEL_PS_PREFIX + "request.resource.use.minibatch";
  public static final boolean DEFAULT_ANGEL_PS_REQUEST_RESOURCE_USE_MINIBATCH = false;

  public static final String ANGEL_PS_REQUEST_RESOURCE_MINIBATCH_SIZE = ANGEL_PS_PREFIX + "request.resource.minibatch.size";
  public static final int DEFAULT_ANGEL_PS_REQUEST_RESOURCE_MINIBATCH_SIZE = 1;

  public static final String ANGEL_PS_REQUEST_RESOURCE_MINIBATCH_INTERVAL_MS = ANGEL_PS_PREFIX + "request.resource.minibatch.interval.ms";
  public static final int DEFAULT_ANGEL_PS_REQUEST_RESOURCE_MINIBATCH_INTERVAL_MS = 8000;

  /**
   * The maximum time in milliseconds for AppMaster waiting for heartbeats from pss. Once a ps does
   * not send heartbeat to AppMaster during setting time, it can be considered that the ps has been
   * down.
   */
  public static final String ANGEL_PS_HEARTBEAT_TIMEOUT_MS =
      ANGEL_PS_PREFIX + "heartbeat.timeout.ms";
  public static final long DEFAULT_ANGEL_PS_HEARTBEAT_TIMEOUT_MS = 600000;

  public static final String ANGEL_PS_HEARTBEAT_DATA_COLLECTION_INTERVAL = ANGEL_PS_PREFIX + "heartbeat.data.collection.interval";
  public static final int DEFAULT_ANGEL_PS_HEARTBEAT_DATA_COLLECTION_INTERVAL = 5;

  /**
   * PS executors thread pool size
   */
  public static final String ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE =
      ANGEL_PS_PREFIX + "matrix.diskio.worker.pool.size";

  /**
   * Default PS executors thread pool size
   */
  public static final int DEFAULT_ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE =
      Math.max(16, (int) (Runtime.getRuntime().availableProcessors() * 0.5));

  public static final String ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE =
      ANGEL_PS_PREFIX + "max.partition.number.single.file";
  public static final int DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE = 5;

  /**
   * Server Partition source data Class
   */
  public static final String ANGEL_PS_PARTITION_SOURCE_CLASS =
      ANGEL_PS_PREFIX + "partition.source.class";
  public static final String DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS =
      PartitionSourceMap.class.getName();

  /**
   * partitionClass for matrix
   */

  public static final String Angel_PS_PARTITION_CLASS =
      ANGEL_PS_PREFIX + "partition.class";

  public static final String ANGEL_PS_CHECKPOINTS_MAX_NUM = ANGEL_PS_PREFIX + "checkpoint.max.num";
  public static final int DEFAULT_ANGEL_PS_CHECKPOINTS_MAX_NUM = 1;

  public static final String ANGEL_PS_MAX_LOCK_WAITTIME_MS = ANGEL_PS_PREFIX + "max.lock.waittime";
  public static final int DEFAULT_ANGEL_PS_MAX_LOCK_WAITTIME_MS = 10000;

  public static final String ANGEL_PS_USE_ADAPTIVE_KEY_ENABLE =
      ANGEL_PS_PREFIX + "use.adaptive.key.enable";
  public static final boolean DEFAULT_ANGEL_PS_USE_ADAPTIVE_KEY_ENABLE = true;

  public static final String ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE =
      ANGEL_PS_PREFIX + "use.adaptive.storage.enable";
  public static final boolean DEFAULT_ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE = false;

  public static final String ANGEL_PS_SPARSE_TO_DENSE_FACTOR =
      ANGEL_PS_PREFIX + "sparse.to.dense.factor";
  public static final float DEFAULT_ANGEL_PS_SPARSE_TO_DENSE_FACTOR = 0.25f;

  public static final String ANGEL_PS_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF =
      ANGEL_PS_PREFIX + "jvm.direct.factor.use.direct.buff";
  public static final float DEFAULT_ANGEL_PS_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF = 0.15f;

  public static final String ANGEL_PS_JVM_DIRECT_FACTOR_USE_HEAP_BUFF =
      ANGEL_PS_PREFIX + "jvm.direct.factor.use.heap.buff";
  public static final float DEFAULT_ANGEL_PS_JVM_DIRECT_FACTOR_USE_HEAP_BUFF = 0.10f;

  public static final String ANGEL_PS_JVM_YOUNG_FACTOR = ANGEL_PS_PREFIX + "jvm.young.factor";
  public static final float DEFAULT_ANGEL_PS_JVM_YOUNG_FACTOR = 0.15f;


  public static final String ANGEL_PS_JVM_USE_PARALLEL_GC = ANGEL_PS_PREFIX + "jvm.use.parallel.gc";
  public static final boolean DEFAULT_ANGEL_PS_JVM_USE_PARALLEL_GC = false;

  public static final String ANGEL_PS_JVM_PARALLEL_GC_THREADS = ANGEL_PS_PREFIX + "jvm.parallel.gc.threads";
  public static final int DEFAULT_ANGEL_PS_JVM_PARALLEL_GC_THREADS = 4;

  public static final String ANGEL_PS_JVM_PARALLEL_GC_MAX_PAUSE_TIME_MS = ANGEL_PS_PREFIX + "jvm.parallel.gc.max.pause.time.ms";
  public static final int DEFAULT_ANGEL_PS_JVM_PARALLEL_GC_MAX_PAUSE_TIME_MS = 1000;

  public static final String ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE = ANGEL_PS_PREFIX + "jvm.parallel.gc.use.adaptive.size";
  public static final boolean DEFAULT_ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE = false;

  /**
   * Enable G1 in PS
   */
  public static final String ANGEL_PS_JVM_USE_G1 = ANGEL_PS_PREFIX + "jvm.use.g1";
  public static final boolean DEFAULT_ANGEL_PS_JVM_USE_G1 = true;

  /**
   * Max GC pause time
   */
  public static final String ANGEL_PS_JVM_G1_MAXPAUSETIME_MS =
      ANGEL_PS_PREFIX + "jvm.g1.maxpausetime.ms";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_MAXPAUSETIME_MS = 2000;

  /**
   * Minimal young region ratio
   */
  public static final String ANGEL_PS_JVM_G1_MIN_NEWRATIO = ANGEL_PS_PREFIX + "jvm.g1.min.newratio";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_MIN_NEWRATIO = 5;

  /**
   * Maximal young region ratio
   */
  public static final String ANGEL_PS_JVM_G1_MAX_NEWRATIO = ANGEL_PS_PREFIX + "jvm.g1.max.newratio";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_MAX_NEWRATIO = 60;

  /**
   * Region size
   */
  public static final String ANGEL_PS_JVM_G1_REGIONSIZE_MB =
      ANGEL_PS_PREFIX + "jvm.g1.regionsize.mb";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_REGIONSIZE_MB = 32;

  /**
   * InitiatingHeapOccupancyPercent
   */
  public static final String ANGEL_PS_JVM_G1_IHOP = ANGEL_PS_PREFIX + "jvm.g1.ihop";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_IHOP = 60;

  /**
   * G1MixedGCLiveThresholdPercent
   */
  public static final String ANGEL_PS_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT =
      ANGEL_PS_PREFIX + "jvm.g1.mixgc.live.threshold.percent";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT = 65;

  /**
   * G1MixedGCCountTarget
   */
  public static final String ANGEL_PS_JVM_G1_MIXGC_TARGET_COUNT =
      ANGEL_PS_PREFIX + "jvm.g1.mixgc.target.count";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_MIXGC_TARGET_COUNT = 8;

  /**
   * ParallelGCThreads
   */
  public static final String ANGEL_PS_JVM_G1_WORKER_NUM = ANGEL_PS_PREFIX + "jvm.g1.worker.num";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_WORKER_NUM = 4;

  /**
   * ConcGCThreads
   */
  public static final String ANGEL_PS_JVM_G1_CONC_WORKER_NUM =
      ANGEL_PS_PREFIX + "jvm.g1.conc.worker.num";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_CONC_WORKER_NUM = 4;

  /**
   * G1ReservePercent
   */
  public static final String ANGEL_PS_JVM_G1_RESERVE_PERCENT =
      ANGEL_PS_PREFIX + "jvm.g1.reserve.percent";
  public static final int DEFAULT_ANGEL_PS_JVM_G1_RESERVE_PERCENT = 10;

  /**
   * Pin the PS heap to its max size at JVM start (set {@code -Xms == -Xmx}). Without this G1
   * reserves only {@code -Xmx} as the upper bound and grows the committed heap on demand;
   * each expansion event {@code mmap}s and zero-fills several gigabytes inside a stop-the-
   * world pause. We have observed back-to-back STW pauses of 0.85s / 1.45s / 1.59s / 1.51s
   * (yes, 5 seconds aggregate) during ramp-up where the heap grew 2G → 4G → 8G → 12.7G →
   * 16.3G; clients to the PS interpret those pauses as silent timeouts and the connection
   * pool tears itself down. Pre-committing the heap up front trades a one-off slower JVM
   * start for a permanently flat GC pause distribution. Strongly recommended for production.
   */
  public static final String ANGEL_PS_JVM_G1_PIN_HEAP_SIZE =
      ANGEL_PS_PREFIX + "jvm.g1.pin.heap.size";
  public static final boolean DEFAULT_ANGEL_PS_JVM_G1_PIN_HEAP_SIZE = true;

  /**
   * Touch every heap page at JVM start so the kernel actually backs the reserved virtual
   * memory with physical pages and zeroes them up front. Combined with {@link
   * #ANGEL_PS_JVM_G1_PIN_HEAP_SIZE} this completely removes the "expand heap" code path
   * from the steady-state STW budget. Costs one extra ~10s at JVM startup (linearly with
   * heap size); on a 16G PS that is dwarfed by the multi-second GC pauses it prevents.
   */
  public static final String ANGEL_PS_JVM_G1_ALWAYS_PRE_TOUCH =
      ANGEL_PS_PREFIX + "jvm.g1.always.pre.touch";
  public static final boolean DEFAULT_ANGEL_PS_JVM_G1_ALWAYS_PRE_TOUCH = true;

  /**
   * Process java.lang.ref.Reference instances in parallel during STW pauses. The default
   * single-threaded path can dominate the {@code [Ref Proc: ...]} component of a pause
   * once the heap has accumulated many soft / weak / phantom references — common on
   * long-running PS JVMs hosting matrix shards backed by direct buffers.
   */
  public static final String ANGEL_PS_JVM_G1_PARALLEL_REF_PROC =
      ANGEL_PS_PREFIX + "jvm.g1.parallel.ref.proc";
  public static final boolean DEFAULT_ANGEL_PS_JVM_G1_PARALLEL_REF_PROC = true;

  /**
   * Reserved-for-non-heap memory inside the PS container, in MB. The PS JVM's total RSS is
   * approximately {@code -Xmx + -XX:MaxDirectMemorySize + this overhead}, where the
   * overhead covers Metaspace, code cache, thread stacks, native libraries (libhdfs, netty
   * epoll, libtcmalloc, etc.), JVM-internal data structures, and direct-buffer headroom
   * that is not pre-reserved.
   *
   * <p>Historically the code subtracted a fixed 512MB which is far too small for a modern
   * deployment running netty + hadoop native + various ML kernels — the resulting RSS
   * routinely overshoots the container quota and the NodeManager SIGKILLs the JVM (visible
   * as {@code Container killed on request. Exit code is 137 / Killed by external signal}
   * with no PS-side stack trace because the kernel reaps the process immediately). Default
   * raised to 1024MB and made configurable so operators can grow it on heavyweight PS
   * shards without touching code. Set to a value strictly less than the container memory.
   */
  public static final String ANGEL_PS_JVM_OVERHEAD_MB =
      ANGEL_PS_PREFIX + "jvm.overhead.mb";
  public static final int DEFAULT_ANGEL_PS_JVM_OVERHEAD_MB = 1024;

  /**
   * <b>Fault injection (test-only).</b> Comma-separated list of integer PS ids whose
   * {@code GetChannelContext.consecutiveInactiveWindows} should be force-incremented at
   * the top of every {@code checkChannelContext} tick, regardless of real traffic. This
   * is the only supported way to trigger the STUCK_PS recovery path on demand, e.g. for
   * regression testing the "PS dies and is replaced — client must follow" scenario without
   * having to actually kill a YARN container. Empty (default) disables the mechanism.
   *
   * <p>Example: {@code angel.psagent.fault.inject.stuck.ps.ids=37,42}.
   *
   * <p>Behaviour: every check tick, for each listed PS id, the corresponding
   * GetChannelContext (if one exists) gets {@code incConsecutiveInactiveWindows()} called
   * an extra time. Three ticks after the configuration takes effect the regular
   * {@code consecutive >= 3} branch fires and exercises the entire fail-safe path
   * (STUCK_PS metric, failedPSToLocMap.put, immediate runPSLocRefreshOnce submit).
   *
   * <p>Strictly NOT for production use: enabling this will trigger spurious channel pool
   * teardowns on the listed PSes once per second and will spam the master with location
   * refresh requests.
   */
  public static final String ANGEL_PSAGENT_FAULT_INJECT_STUCK_PS_IDS =
      "angel.psagent.fault.inject.stuck.ps.ids";
  public static final String DEFAULT_ANGEL_PSAGENT_FAULT_INJECT_STUCK_PS_IDS = "";


    // //////////////////////////////
    // Kubernetes Configs.
    // //////////////////////////////
    /**
     * whether to wait for the application to finish before exiting the launcher process.
     */
    public static final String ANGEL_KUBERNETES_WAIT_FOR_APP_COMPLETION = ANGEL_KUBERNETES_PREFIX + "submission.waitAppCompletion";
    public static final Boolean DEFAULT_ANGEL_KUBERNETES_WAIT_FOR_APP_COMPLETION = true;

    /**
     * kubernetes api server url.
     */
    public static final String ANGEL_KUBERNETES_MASTER = ANGEL_KUBERNETES_PREFIX + "master";

    /**
     * The namespace that will be used for running the master and executor pods.
     */
    public static final String ANGEL_KUBERNETES_NAMESPACE = ANGEL_KUBERNETES_PREFIX + "namespace";
    public static final String DEFAULT_ANGEL_KUBERNETES_NAMESPACE = "default";

    public static final String ANGEL_KUBERNETES_SERVICEACCOUNT = ANGEL_KUBERNETES_PREFIX + "serviceaccount";
    public static final String DEFAULT_ANGEL_KUBERNETES_SERVICEACCOUNT = "default";

    /**
     * Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent.
     */
    public static final String ANGEL_KUBERNETES_CONTAINER_IMAGE_PULL_POLICY = ANGEL_KUBERNETES_PREFIX + "container.image.pullPolicy";
    public static final String DEFAULT_ANGEL_KUBERNETES_CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";

    /**
     * Kubernetes executor pod role. Valid values are ps, worker
     */
    public static final String ANGEL_KUBERNETES_EXECUTOR_ROLE = ANGEL_KUBERNETES_PREFIX + "executor.role";
    public static final String DEFAULT_ANGEL_KUBERNETES_EXECUTOR_ROLE = "ps";

    /**
     * Prefix to use in front of the executor pod names
     */
    public static final String ANGEL_KUBERNETES_EXECUTOR_POD_NAME_PREFIX = ANGEL_KUBERNETES_PREFIX + "executor.podNamePrefix";

    public static final String ANGEL_KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX = ANGEL_KUBERNETES_PREFIX + "authenticate.submission";

    public static final String ANGEL_KUBERNETES_KUBERNETES_AUTH_CLIENT_MODE_PREFIX = ANGEL_KUBERNETES_PREFIX + "authenticate";

    public static final String OAUTH_TOKEN_CONF_SUFFIX = "oauthToken";

    public static final String OAUTH_TOKEN_FILE_CONF_SUFFIX = "oauthTokenFile";

    public static final String CLIENT_KEY_FILE_CONF_SUFFIX = "clientKeyFile";

    public static final String CLIENT_CERT_FILE_CONF_SUFFIX = "clientCertFile";

    public static final String CA_CERT_FILE_CONF_SUFFIX = "caCertFile";

    /**
     * Interval between reports of the current app status, Logging interval must be a positive time value.
     */
    public static final String ANGEL_KUBERNETES_REPORT_INTERVAL = ANGEL_KUBERNETES_PREFIX + "report.interval";
    public static final int DEFAULT_ANGEL_KUBERNETES_REPORT_INTERVAL = 1000;

    /**
     * Interval between successive inspection of executor events sent from the Kubernetes API.
     */
    public static final String ANGEL_KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL = ANGEL_KUBERNETES_PREFIX + "executor.eventProcessingInterval";
    public static final int DEFAULT_ANGEL_KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL = 1000;

    /**
     * Container image to use for Angel containers.
     */
    public static final String ANGEL_KUBERNETES_CONTAINER_IMAGE = ANGEL_KUBERNETES_PREFIX + "container.image";

    /**
     * master and executor pods extra classpath
     */
    public static final String ANGEL_KUBERNETES_MASTER_EXTRA_CALSSPATH = ANGEL_KUBERNETES_PREFIX + "master.extraClassPath";

    public static final String ANGEL_KUBERNETES_EXECUTOR_EXTRA_CALSSPATH = ANGEL_KUBERNETES_PREFIX + "executor.extraClassPath";

    /**
     * Specify the hard cpu limit for the angel pods
     */
    public static final String ANGEL_KUBERNETES_MASTER_LIMIT_CORES = ANGEL_KUBERNETES_PREFIX + "master.limit.cores";

    public static final String ANGEL_KUBERNETES_PS_LIMIT_CORES = ANGEL_KUBERNETES_PREFIX + "ps.limit.cores";

    public static final String ANGEL_KUBERNETES_WORKER_LIMIT_CORES = ANGEL_KUBERNETES_PREFIX + "worker.limit.cores";

    public static final String ANGEL_KUBERNETES_MASTER_PORT = ANGEL_KUBERNETES_PREFIX + "master.port";
    public static final int DEFAULT_ANGEL_KUBERNETES_MASTER_PORT = 9078;

    public static final String ANGEL_KUBERNETES_MASTER_POD_IP = ANGEL_KUBERNETES_PREFIX + "master.pod.ip";

    /**
     * Number of pods to launch at once in each round of executor allocation.
     */
    public static final String ANGEL_KUBERNETES_ALLOCATION_BATCH_SIZE = ANGEL_KUBERNETES_PREFIX + "allocation.batch.size";
    public static final int DEFAULT_ANGEL_KUBERNETES_ALLOCATION_BATCH_SIZE = 5;

    /**
     * Time to wait between each round of executor allocation.
     */
    public static final String ANGEL_KUBERNETES_ALLOCATION_BATCH_DELAY = ANGEL_KUBERNETES_PREFIX + "allocation.batch.delay";
    public static final int DEFAULT_ANGEL_KUBERNETES_ALLOCATION_BATCH_DELAY = 1000;

    /**
     * Name of the angel master pod
     */
    public static final String ANGEL_KUBERNETES_MASTER_POD_NAME = ANGEL_KUBERNETES_PREFIX + "master.pod.name";

    /**
     * Interval between polls against the Kubernetes API server to inspect the state of executors.
     */
    public static final String ANGEL_KUBERNETES_EXECUTOR_API_POLLING_INTERVAL = ANGEL_KUBERNETES_PREFIX + "executor.apiPollingInterval";
    public static final int DEFAULT_ANGEL_KUBERNETES_EXECUTOR_API_POLLING_INTERVAL = 30000;

    public static final String ANGEL_KUBERNETES_APP_ID = ANGEL_KUBERNETES_PREFIX + "app.id";
    public static final String ANGEL_KUBERNETES_APP_CLUSTERTIMESTAMP = ANGEL_KUBERNETES_PREFIX + "app.clusterTimestamp";
    public static final String ANGEL_KUBERNETES_APP_RANDOMID = ANGEL_KUBERNETES_PREFIX + "app.randomId";

  // ////////////////// IPC //////////////////////////
  /**
   * The read buffer size for rpc message encoded by protobuf.
   */
  public static final String ANGEL_REQUEST_PB_READBUFFER_LIMIT = "request.pb.readbuffer.limit";
  public static final int DEFAULT_ANGEL_REQUEST_PB_READBUFFER_LIMIT = 64 << 20;

  /**
   * The maximum package size for a rpc message.
   */
  public static final String ANGEL_RPC_MAX_PACKAGE_SIZE = "angel.rpc.package.max.size";
  public static final int DEFAULT_ANGEL_RPC_MAX_PACKAGE_SIZE = 4 << 20;

  /**
   * The maximum time in seconds waiting for a rpc response.
   */
  public static final String ANGEL_READ_TIMEOUT_SEC = "angel.read.timeout.sec";
  public static final int DEFAULT_ANGEL_READ_TIMEOUT_SEC = 100;

  /**
   * The rpc retry time interval in milliseconds.
   */
  public static final String ANGEL_REQUEST_SLEEP_TIME_MS = "angel.request.sleep.time.ms";
  public static final int DEFAULT_ANGEL_REQUEST_SLEEP_TIME_MS = 1000;

  /**
   * Key router type: hash or range
   */
  public static final String ANGEL_PS_ROUTER_TYPE = ANGEL_PS_PREFIX + "router.type";
  public static final String DEFAULT_ANGEL_PS_ROUTER_TYPE = "hash";

  /**
   * Key router type: hash or range
   */
  public static final String ANGEL_PS_LOADBALANCE_PARTITION_ENABLE = ANGEL_PS_PREFIX + "router.type";
  public static final boolean DEFAULT_ANGEL_PS_LOADBALANCE_PARTITION_ENABLE = true;

  // //////////////////////////////
  // Matrix transfer Configs.
  // //////////////////////////////

  /**
   * The maximum message size in a matrix transfer rpc.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE =
      "angel.netty.matrixtransfer.max.message.size";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE = 100 * 1024 * 1024;
  ;

  /**
   * The eventgroup thread number for netty client for matrix transfer.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM =
      "angel.netty.matrixtransfer.client.eventgroup.threadnum";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM =
      Math.max(8, (int) (Runtime.getRuntime().availableProcessors() * 0.25));

  /**
   * The send buffer size for netty client for matrix transfer.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF =
      "angel.netty.matrixtransfer.client.sndbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF = 1024 * 1024;

  /**
   * The receive buffer size for netty client for matrix transfer.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF =
      "angel.netty.matrixtransfer.client.rcvbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF = 1024 * 1024;

  /**
   * The max number of channels to a ps.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_PERSERVER =
      "angel.netty.matrixtransfer.client.max.connect.perserver";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_PERSERVER = 5;

  /**
   * The min number of channels to a ps.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MIN_CONN_PERSERVER =
      "angel.netty.matrixtransfer.client.min.connect.perserver";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MIN_CONN_PERSERVER = 3;

  /**
   * The get channel timeout in milliseconds.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_GET_CONN_TIMEOUT_MS =
      "angel.netty.matrixtransfer.client.max.get.connect.timeout.ms";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_GET_CONN_TIMEOUT_MS = 30000;

  /**
   * The max idle time for a channel
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_IDLETIME_MS =
      "angel.netty.matrixtransfer.client.max.connect.idletime.ms";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_IDLETIME_MS = 3000000;

  /**
   * Netty channel io ratio
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_IORATIO =
      "angel.netty.matrixtransfer.client.ioratio";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_IORATIO = 80;

  /**
   * Netty Channel Type: nio or epoll
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_CHANNEL_TYPE =
      "angel.netty.matrixtransfer.client.channel.type";
  public static final String DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_CHANNEL_TYPE = "nio";

  /**
   * Netty channel io ratio
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_IORATIO =
      "angel.netty.matrixtransfer.server.ioratio";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_IORATIO = 80;

  /**
   * Netty Channel Type: nio or epoll
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_TYPE =
      "angel.netty.matrixtransfer.server.channel.type";
  public static final String DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_TYPE = "nio";

  /**
   * If true, the matrix-transfer server will <em>automatically</em> upgrade from NIO to Epoll
   * on Linux when the netty-transport-native-epoll classes are available, regardless of
   * {@link #ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_TYPE}. The historical default required
   * the operator to set channel.type=epoll explicitly, which most deployments forget to do
   * and silently miss out on Epoll edge-trigger / SO_REUSEPORT / fewer syscalls (typically
   * 15-25% lower tail latency under load).
   *
   * <p>Set to false to keep the legacy behaviour ("only honour channel.type"). Setting
   * channel.type=nio + auto.epoll=true still upgrades to epoll on Linux. Setting
   * channel.type=epoll on a non-Linux box still falls back to NIO (epoll is Linux-only).
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_AUTO_EPOLL =
      "angel.netty.matrixtransfer.server.channel.auto.epoll";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_AUTO_EPOLL = true;

  /**
   * TCP listen backlog (SO_BACKLOG) for the matrix-transfer server. Netty's default is
   * NetUtil.SOMAXCONN (typically 128 on Linux), which is silently truncated by the kernel
   * to /proc/sys/net/core/somaxconn. With hundreds of executors reconnecting after a
   * task-retry burst this small queue overflows and the kernel responds with TCP RST,
   * surfacing as "Connection reset by peer" in client logs and forcing another retry.
   * Setting this to 1024 (or higher) gives the accept queue enough headroom to absorb
   * connection storms without dropping SYNs.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_BACKLOG =
      "angel.netty.matrixtransfer.server.backlog";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_BACKLOG = 1024;

  /**
   * Whether to enable {@code TCP_NODELAY} (disable Nagle) on accepted child channels of the
   * matrix-transfer server. Nagle batches small writes for up to 200ms waiting for
   * additional data, which is the worst possible behaviour for an RPC where the request
   * latency matters more than throughput on small responses. Already enabled on the
   * matrix-transfer client side; mirroring it on the server prevents the server-side
   * Nagle delay from showing up as inflated PSF response wall-clock for tiny PSFs (e.g.
   * dst-side batches of 1-2 seeds in PPR).
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_TCP_NODELAY =
      "angel.netty.matrixtransfer.server.tcp.nodelay";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_TCP_NODELAY = true;

  /**
   * Netty {@link io.netty.channel.WriteBufferWaterMark} HIGH watermark for matrix-transfer
   * server child channels (in bytes). When the per-channel outbound bytes exceed this,
   * {@code Channel.isWritable()} flips to false. Note that {@code WorkerPool.sendResult}
   * does NOT consult {@code isWritable()} today, so this is currently a soft cap that
   * only matters as the OOM safety boundary: with N concurrently-busy channels each
   * holding up to HIGH bytes of unflushed PSF responses, total direct memory committed
   * to outbound is bounded by {@code N * HIGH}.
   *
   * <p>Default 32MB. The matrix-transfer server typically faces hundreds of executor
   * channels at once on graph workloads (PPR, Node2Vec, GraphSAGE), and PSF responses
   * commonly reach 20-30MB. Holding 32MB per channel leaves room for one in-flight
   * response plus partial spillover before back-pressure kicks in. The previous default
   * of 64MB was unsafe in fan-out: 100 channels × 64MB = 6.4GB direct memory at peak,
   * which routinely OOMed PSes whose direct-memory budget was sized for steady state.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_HIGH_WATERMARK =
      "angel.netty.matrixtransfer.server.write.buffer.high.watermark";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_HIGH_WATERMARK =
      32 * 1024 * 1024;

  /**
   * Netty {@link io.netty.channel.WriteBufferWaterMark} LOW watermark for matrix-transfer
   * server child channels (in bytes). Once the channel goes unwritable (HIGH crossed),
   * Netty will flip it back to writable only after the outbound bytes fall below LOW.
   * Default 8MB (= HIGH/4) to avoid flip-flopping when the response stream is bursty.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_LOW_WATERMARK =
      "angel.netty.matrixtransfer.server.write.buffer.low.watermark";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_LOW_WATERMARK =
      8 * 1024 * 1024;

  /**
   * The eventgroup thread number for netty server for matrix transfer.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM =
      "angel.netty.matrixtransfer.server.eventgroup.threadnum";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM =
      Math.max(16, (int) (Runtime.getRuntime().availableProcessors() * 0.5));


  /**
   * The eventgroup thread number for netty server for serving transfer.
   */
  public static final String ANGEL_NETTY_SERVING_TRANSFER_SERVER_EVENTGROUP_THREADNUM =
      "angel.netty.serving.transfer.server.eventgroup.threadnum";
  public static final int DEFAULT_ANGEL_NETTY_SERVING_TRANSFER_SERVER_EVENTGROUP_THREADNUM =
      Runtime.getRuntime().availableProcessors() * 2;

  /**
   * The send buffer size for netty server for matrix transfer.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF =
      "angel.netty.matrixtransfer.server.sndbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF = 1024 * 1024;

  /**
   * The receive buffer size for netty server for matrix transfer.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF =
      "angel.netty.matrixtransfer.server.rcvbuf";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF = 1024 * 1024;

  /**
   * The maximum allowed number of matrix transfer requests which are sending to a single
   * server(ps). It used to flow-control between psagent and ps.
   */
  public static final String ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER =
      ANGEL_PREFIX + "matrixtransfer.max.requestnum.perserver";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER = 8;

  public static final String ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE =
      ANGEL_PREFIX + "matrixtransfer.client.requester.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE =
      Math.max(16, (int) (Runtime.getRuntime().availableProcessors() * 0.25));

  public static final String ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE =
      ANGEL_PREFIX + "matrixtransfer.client.responser.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE =
      Math.max(8, (int) (Runtime.getRuntime().availableProcessors() * 0.25));


  public static final String ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE =
      ANGEL_PREFIX + "matrixtransfer.server.worker.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE =
      Math.max(16, (int) (Runtime.getRuntime().availableProcessors() * 0.25));

  public static final String ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS =
      ANGEL_PREFIX + "matrixtransfer.server.token.timeout.ms";

  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS = 10000;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR =
      ANGEL_PREFIX + "matrixtransfer.server.rpc.limit.factor";
  public static final float DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR = 128.0f;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR =
      ANGEL_PREFIX + "matrixtransfer.server.rpc.limit.general.factor";
  public static float DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR = 0.6f;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_USE_ASYNC_HANDLER =
      ANGEL_PREFIX + "matrixtransfer.server.use.async.handler";
  public static final boolean DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USE_ASYNC_HANDLER = true;

  /**
   * If true, the matrix-transfer server records per-{@link
   * com.tencent.angel.ps.server.data.TransportMethod} RPC counters (call count, error
   * count, accumulated handle latency, accumulated request/response bytes, max handle
   * latency) and prints a delta summary to the PS log every
   * {@link #ANGEL_MATRIXTRANSFER_SERVER_METRICS_LOG_INTERVAL_MS} ms.
   *
   * <p>The recording path is allocation-free and uses lock-free AtomicLong counters,
   * so the overhead per RPC is a handful of {@code lazySet} writes (sub-microsecond).
   * Disable only if you are absolutely sure the cost matters and you have profiling
   * data; this is far cheaper than full Dropwizard / Prometheus instrumentation.
   */
  public static final String ANGEL_MATRIXTRANSFER_SERVER_METRICS_ENABLE =
      ANGEL_PREFIX + "matrixtransfer.server.metrics.enable";
  public static final boolean DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_ENABLE = true;

  /**
   * Periodic interval (millis) at which {@link
   * com.tencent.angel.ps.server.data.profiling.MatrixTransportServerMetrics} flushes a
   * delta summary line to the PS log. Set to 0 or negative to record but never
   * auto-print (useful when an external scraper queries the counters).
   * Default 60_000 (one minute).
   */
  public static final String ANGEL_MATRIXTRANSFER_SERVER_METRICS_LOG_INTERVAL_MS =
      ANGEL_PREFIX + "matrixtransfer.server.metrics.log.interval.ms";
  public static final long DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_LOG_INTERVAL_MS = 60_000L;

  /**
   * If true, PSF-class-level breakdown is collected on top of the per-{@link
   * com.tencent.angel.ps.server.data.TransportMethod} aggregate. Useful for graph /
   * embedding workloads where {@code GET_PSF} is dominated by a handful of specific
   * PSFs (e.g. {@code SampleWithTagSamples} for PPR) and the operator needs to know
   * which one is the long pole.
   *
   * <p>Hot-path cost: one {@code ConcurrentHashMap.get(String)} lookup per PSF call
   * (returning a shared bucket after first sight), plus the same handful of
   * AtomicLong adds as the global counters. The map is populated only on first sight
   * of each PSF class — typical workloads see &lt; 100 distinct PSF FQCNs across the
   * whole job, so the steady-state cost is a single hash + equals comparison.
   */
  public static final String ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_ENABLE =
      ANGEL_PREFIX + "matrixtransfer.server.metrics.psf.enable";
  public static final boolean DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_ENABLE = true;

  /**
   * Cap on the number of PSF rows printed in each periodic summary, sorted by tick
   * handleNanos descending. Keeps the log readable when a workload exercises dozens
   * of PSF classes. Set to 0 / negative to print everything ever seen.
   */
  public static final String ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_TOP_N =
      ANGEL_PREFIX + "matrixtransfer.server.metrics.psf.top.n";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_TOP_N = 20;

  /**
   * If &gt; 0 and {@link #ANGEL_MATRIXTRANSFER_SERVER_USE_ASYNC_HANDLER} is true, PSF
   * requests whose {@code handleElemNum} (number of elements requested) is &le; this
   * threshold are processed inline on the netty IO thread, skipping the dispatch to the
   * RPCWorker pool. Saves one queue-and-wakeup per request, which is a meaningful tail
   * latency win for tiny PSFs (typical usage: e.g. PPR dst-side batches of 1-2 seeds
   * where the per-step PS-side work is microsecond-level but currently still pays a full
   * thread-pool round-trip per RPC).
   *
   * <p>Default 0 (== always async, historical behaviour). Setting this &gt; 0 is risky if
   * any registered PSF is CPU-heavy even for small element counts: blocking on the IO
   * thread freezes every other channel pinned to the same event loop. Profile first.
   */
  public static final String ANGEL_MATRIXTRANSFER_SERVER_SYNC_THRESHOLD =
      ANGEL_PREFIX + "matrixtransfer.server.sync.threshold";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SYNC_THRESHOLD = 0;

  public static final String ANGEL_MATRIXTRANSFER_SERVER_USE_UNLOCK_RPC =
      ANGEL_PREFIX + "matrixtransfer.server.use.unlock.rpc";

  public static final Boolean DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USE_UNLOCK_RPC = false;

  public static final String ANGEL_PS_USE_INDEPENDENT_WORKER_POOL = "angel.ps.use.independent.worker.pool";
  public static final boolean DEFAULT_ANGEL_PS_USE_INDEPENDENT_WORKER_POOL = true;


  public static final String ANGEL_MATRIX_OPLOG_MERGER_POOL_SIZE =
      ANGEL_PREFIX + "matrix.oplog.merger.pool.size";

  public static final int DEFAULT_ANGEL_MATRIX_OPLOG_MERGER_POOL_SIZE =
      Math.max(8, (int) (Runtime.getRuntime().availableProcessors() * 0.25));

  /**
   * The maximum allowed number of matrix transfer requests which are sending to the servers(ps). It
   * used to flow-control between psagent and ps.
   */
  public static final String ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM =
      ANGEL_PREFIX + "matrixtransfer.max.requestnum";
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
  public static final String ANGEL_REFRESH_SERVERLOCATION_THRESHOLD =
      ANGEL_PREFIX + "refresh.serverlocation.threshold";
  public static final int DEFAULT_ANGEL_REFRESH_SERVERLOCATION_THRESHOLD = 5;

  /**
   * The time interval in milliseconds for failed matrix transfer requests.
   */
  public static final String ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS =
      ANGEL_PREFIX + "matrixtransfer.retry.interval.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS = 10000;

  /**
   * Max retry number for a PS-RPC
   */
  public static final String ANGEL_MATRIXTRANSFER_MAX_TRY_COUNTER =
      ANGEL_PREFIX + "matrixtransfer.max.retry.counter";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_MAX_TRY_COUNTER = 10;

  /**
   * The retry backoff window in milliseconds. Used by {@code
   * MatrixTransportClient.RequestDispatcher#genNextRetryTs} as the upper bound of the random
   * back-off for a failed request (next retry time = now + Random(0, this)). Historically the
   * code reused {@code request.timeout.ms} for this purpose, which made network-error retries
   * wait up to several minutes when {@code request.timeout.ms} was tuned large for heavy PSFs.
   * Default 5s gives sub-second average recovery for transient network glitches; raise it if
   * the cluster legitimately needs longer back-off.
   */
  public static final String ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS =
      ANGEL_PREFIX + "matrixtransfer.retry.backoff.window.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS = 5000;

  /**
   * The interval in milliseconds at which the client-side channel pool ({@link
   * com.tencent.angel.common.transport.ChannelManager2}) runs its background checker to evict
   * dead/idle channels and replace dead ones with freshly-connected channels. This is the only
   * way the pool actively notices that a long-lived connection has been silently dropped by an
   * intermediate NAT / firewall when no application traffic flows on it.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_POOL_CHECK_INTERVAL_MS =
      "angel.netty.matrixtransfer.client.pool.check.interval.ms";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_POOL_CHECK_INTERVAL_MS = 30000;

  /**
   * The server-side per-channel idle timeout in seconds. If neither read nor write happens on
   * a netty channel for this many seconds, {@link
   * com.tencent.angel.ps.server.data.MatrixTransportServerHandler} will close the channel. The
   * client immediately receives {@code channelInactive}, invalidates the pool entry, and the
   * next request gets a freshly connected channel that cannot have been timed out by any
   * intermediate device. Set to 0 to disable. Default 120s sits well below typical 5~15min
   * conntrack / cloud-LB idle timeouts.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_IDLE_SECONDS =
      "angel.netty.matrixtransfer.server.channel.idle.seconds";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_IDLE_SECONDS = 120;

  /**
   * The client-side writer-idle interval in seconds. When a client-side netty channel sees no
   * write for this many seconds, an application-level PING (a 0-byte length-prepended frame)
   * is automatically pushed onto the channel by
   * {@link com.tencent.angel.psagent.matrix.transport.ClientHeartbeatHandler}. This keeps any
   * NAT / cloud LB / stateful firewall in the path from silently reaping the long-lived TCP
   * session due to inactivity — the single biggest contributor to the
   * "channel ... is not active" / "is dead, will replace" storms.
   *
   * <p>Set this strictly below both:
   * <ul>
   *   <li>the most aggressive intermediate idle-timeout in the network path
   *       (typical: 5min for cloud SLB, 10min for conntrack), and</li>
   *   <li>{@link #ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_IDLE_SECONDS} (so the server's
   *       all-idle close-on-idle never fires while the client still considers the channel
   *       healthy).</li>
   * </ul>
   *
   * <p>Set to 0 to disable. Default 30s gives a comfortable margin below the default
   * 120s server idle and any production-grade middlebox idle timeout.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_WRITER_IDLE_SECONDS =
      "angel.netty.matrixtransfer.client.writer.idle.seconds";
  public static final int DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_WRITER_IDLE_SECONDS = 30;

  /**
   * Bound on the requester thread pool's work queue. Each queued task carries a partition
   * request which, after serialization, occupies a ByteBuf (often in direct memory). The old
   * {@code Executors.newFixedThreadPool} default of an unbounded LinkedBlockingQueue meant a
   * burst of failed sends could pile up GBs of pending requests and OOM the JVM. With a bound
   * we instead apply {@code CallerRunsPolicy} so back-pressure travels up to the dispatcher.
   */
  public static final String ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_QUEUE_CAPACITY =
      ANGEL_PREFIX + "matrixtransfer.client.requester.queue.capacity";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_QUEUE_CAPACITY = 4096;

  /**
   * Bound on the responder thread pool's work queue. Each queued task owns a response ByteBuf
   * which can be tens of MB for large PSF results — left unbounded, a slow merge can pile up
   * direct memory until {@code OutOfDirectMemoryError}. When the queue is full we apply
   * {@code CallerRunsPolicy} so the netty IO thread itself runs the deserialization, which
   * naturally back-pressures the TCP receive window and slows the server down.
   */
  public static final String ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_QUEUE_CAPACITY =
      ANGEL_PREFIX + "matrixtransfer.client.responser.queue.capacity";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_QUEUE_CAPACITY = 2048;

  /**
   * Size of the PS-heartbeat thread pool used for gray-server liveness probes. Was hardcoded
   * to 8 with an unbounded queue; expose as a knob and bound the queue so a flood of grayed
   * servers cannot snowball into thread/heap pressure.
   */
  public static final String ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_POOL_SIZE =
      ANGEL_PREFIX + "matrixtransfer.client.heartbeat.pool.size";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_POOL_SIZE = 8;

  public static final String ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_QUEUE_CAPACITY =
      ANGEL_PREFIX + "matrixtransfer.client.heartbeat.queue.capacity";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_QUEUE_CAPACITY = 512;

  /**
   * Weather we need use direct buffer in netty client.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER =
      "angel.netty.matrixtransfer.client.usedirectbuffer";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER = true;

  /**
   * Weather we need use pooled buffer in netty server.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL =
      "angel.netty.matrixtransfer.client.usepool";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL = false;

  /**
   * Weather we need use direct buffer in netty server.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER =
      "angel.netty.matrixtransfer.server.usedirectbuffer";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER = true;

  /**
   * Weather we need use direct buffer in netty server.
   */
  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL =
      "angel.netty.matrixtransfer.server.usepool";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL = false;

  /**
   * The maximum time in milliseconds of waiting for the response. If the response of a request is
   * not received in allowed time, the request will be timeout. The timeout request will be retry
   * later.
   */
  public static final String ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS =
      ANGEL_PREFIX + "matrixtransfer.request.timeout.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS = 120000;

  /**
   * Adapter-level <i>deadline watchdog</i> for user-facing PS requests, in milliseconds.
   * Any {@code FutureResult} that is still un-completed this many milliseconds after its
   * {@code UserRequest} was created will be force-failed so the calling thread blocked
   * inside {@code FutureResult.get()} wakes up with an {@link java.util.concurrent.ExecutionException}
   * instead of hanging forever.
   *
   * <p><b>Why this exists.</b> The per-partition retry path
   * (transport-layer max try counter) only counts <em>actually-sent</em> attempts; a
   * partition request that gets stuck in the failed-cache heap because its target PS is
   * marked as {@code isFailed} and never re-promoted (e.g. the location-refresh chain
   * never fires for it) increments no counter and is never failed. The aggregate user
   * request it belongs to then waits forever, because its {@link FutureResult} is set
   * only when <em>all</em> partition responses arrive or when the per-partition path
   * gives up. We have observed Spark tasks parked on
   * {@code FutureResult.get()} for over an hour in exactly that state. This watchdog is
   * the only universal escape hatch.
   *
   * <p><b>How to size it.</b> Should be strictly larger than the longest legitimate
   * single user request (matrix update / PSF call) seen on the cluster, including a
   * generous margin for retries and back-off. The default 30 minutes is intentionally
   * conservative — we would rather leak a request for half an hour than fail a healthy
   * but slow request mid-flight. Set lower (e.g. 5–10 minutes) on latency-sensitive
   * online-serving deployments, or to 0 to disable entirely.
   */
  public static final String ANGEL_PSAGENT_USER_REQUEST_DEADLINE_MS =
      "angel.psagent.user.request.deadline.ms";
  public static final long DEFAULT_ANGEL_PSAGENT_USER_REQUEST_DEADLINE_MS = 30L * 60L * 1000L;

  /**
   * Period at which the adapter-level deadline watchdog scans all in-flight user requests,
   * in milliseconds. Must be strictly less than
   * {@link #ANGEL_PSAGENT_USER_REQUEST_DEADLINE_MS}; the actual fail-fast delay is then
   * bounded above by {@code deadline + period}. The default 30 s is sufficient for the
   * typical 30-minute deadline.
   */
  public static final String ANGEL_PSAGENT_USER_REQUEST_DEADLINE_CHECK_INTERVAL_MS =
      "angel.psagent.user.request.deadline.check.interval.ms";
  public static final long DEFAULT_ANGEL_PSAGENT_USER_REQUEST_DEADLINE_CHECK_INTERVAL_MS = 30000L;

  /**
   * When {@code true} (default), the responder thread pool's saturation policy converts a
   * rejected response into an in-band {@link com.tencent.angel.ps.server.data.response.ResponseType#SERVER_IS_BUSY}
   * failure on the corresponding seqId, rather than running the {@code Responser} task
   * inline on the netty IO thread (the historical {@code CallerRunsPolicy} behaviour).
   *
   * <p><b>Why this exists.</b> {@code CallerRunsPolicy} is the right back-pressure strategy
   * when the IO thread runs one channel — slowing it slows the TCP receive window for that
   * sender, which is exactly what we want. But every {@code NioEventLoop} multiplexes many
   * channels, and a single multi-second deserialize stalls every other channel pinned to
   * the same loop. We've correlated that with mass channel-inactive bursts after a slow
   * partition response. The {@code SERVER_IS_BUSY} path frees the IO thread instantly,
   * and the affected seqId is retried by the dispatcher's normal back-off — i.e. the
   * back-pressure now travels through the retry mechanism instead of through the IO loop.
   *
   * <p>Set to {@code false} to restore the legacy {@code CallerRunsPolicy} behaviour for
   * regression debugging.
   */
  public static final String ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_FAIL_FAST_ON_REJECT =
      ANGEL_PREFIX + "matrixtransfer.client.responser.fail.fast.on.reject";
  public static final boolean DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_FAIL_FAST_ON_REJECT =
      true;

  /**
   * Per-call timeout (ms) wrapping every master-RPC issued by the PS-location refresher and
   * the heartbeat path (e.g. {@code getPSLocation}, {@code psFailed}, {@code isPSExited},
   * {@code getState}). Without a hard ceiling, a stuck master Netty channel can block the
   * single {@code PSLocRefresher} thread for the OS-default TCP retransmit window — easily
   * tens of seconds to minutes — during which no further PSes can be recovered. We instead
   * dispatch each master RPC to {@code hbThreadPool} and time-bound the wait, so a slow
   * master only fails THIS one refresh attempt; the next 5 s tick retries automatically.
   *
   * <p>The default 5 s is large enough to absorb normal RPC latency and an occasional GC
   * pause on master, but small enough that a partial outage doesn't snowball into a
   * cascading PS-loss event on the client.
   */
  public static final String ANGEL_PSAGENT_MASTER_RPC_TIMEOUT_MS =
      "angel.psagent.master.rpc.timeout.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_MASTER_RPC_TIMEOUT_MS = 5000;

  /**
   * The time interval in milliseconds of clock events. We will check timeout requests and retry
   * failed requests when clock event happened.
   */
  public static final String ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS =
      ANGEL_PREFIX + "matrixtransfer.check.interval.ms";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS = 100;

  /**
   * Capacity of {@code MatrixTransportClient.dispatchMessageQueue}. The queue fans in timer
   * ticks, request-failed/retry transitions, channel-close notifications and server-state
   * events; leaving it unbounded can amplify memory usage under failure storms.
   */
  public static final String ANGEL_MATRIXTRANSFER_DISPATCH_EVENT_QUEUE_CAPACITY =
      ANGEL_PREFIX + "matrixtransfer.dispatch.event.queue.capacity";
  public static final int DEFAULT_ANGEL_MATRIXTRANSFER_DISPATCH_EVENT_QUEUE_CAPACITY = 50000;

  public static final String ANGEL_NETTY_MATRIXTRANSFER_SERVER_USE_STREAM_HANDLER =

      "angel.netty.matrixtransfer.server.use.stream.handler";
  public static final boolean DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USE_STREAM_HANDLER = false;

  // //////////////////////////////
  // Matrix transfer Configs.
  // //////////////////////////////
  public static final String ANGEL_PSAGENT_PREFIX = "angel.psagent.";
  /**
   * PSAgent caches synchronization time interval in milliseconds. The caches contain matrix caches
   * and clock caches, which synchronize data from pss regularly. Matrix caches are used to cache
   * matrix data splits. Clock caches are used to cache the clock values of all matrix partitions.
   */
  public static final String ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS =
      ANGEL_PSAGENT_PREFIX + "cache.sync.timeinterval.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS = 200;

  public static final String ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS =
      ANGEL_PSAGENT_PREFIX + "to.ps.heartbeat.interval.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS = 5000;

  public static final String ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS =
      ANGEL_PSAGENT_PREFIX + "to.ps.heartbeat.timeout.ms";
  public static final int DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS = 600000;

  public static final String ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE =
      ANGEL_PSAGENT_PREFIX + "update.split.adaption.enable";
  public static final boolean DEFAULT_ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE = true;

  public static final String ANGEL_PSAGENT_UPDATE_SPLIT_VIEW_ENABLE =
      ANGEL_PSAGENT_PREFIX + "update.split.view.enable";
  public static final boolean DEFAULT_ANGEL_PSAGENT_UPDATE_SPLIT_VIEW_ENABLE = true;

  /**
   * The machine addresses on which the pss are expected to run. The addressed are separated by
   * commas.
   */
  public static final String ANGEL_PS_IP_LIST = ANGEL_PS_PREFIX + "ip.list";

  /**
   * Weather we need update the matrix clock and task iteration to master synchronously.
   */
  public static final String ANGEL_PSAGENT_SYNC_CLOCK_ENABLE =
      ANGEL_PSAGENT_PREFIX + "sync.clock.enable";
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
  public static final String ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS =
      ANGEL_PSAGENT_PREFIX + "heartbeat.timeout.ms";
  public static final long DEFAULT_ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS = 60000;

  // model parse
  public static final String ANGEL_MODEL_PARSE_THREAD_COUNT = "angel.model.parse.thread.count";
  public static final String ANGEL_MODEL_PARSE_NAME = "angel.model.parse.name";
  public static final String ANGEL_PARSE_MODEL_PATH = "angel.parse.model.path";
  public static final int DEFAULT_ANGEL_MODEL_PARSE_THREAD_COUNT = 5;


  public static final String ML_CONNECTION_TIMEOUT_MILLIS = "ml.connection.timeout";
  /**
   * If not specified, the default connection timeout will be used (3 sec).
   */
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

  public static final String ANGEL_PLUGIN_SERVICE_ENABLE = "angel.plugin.service.enable";
  public static final String ANGEL_SERVING_SHARDING_NUM = "angel.serving.sharding.num";
  public static final String ANGEL_SERVING_SHARDING_CONCURRENT_CAPACITY =
      "angel.serving.sharding.concurrent.capacity";
  public static final String ANGEL_SERVING_SHARDING_MODEL_CLASS =
      "angel.serving.sharding.model.class";
  public static final String ANGEL_SERVING_MASTER_IP = "angel.serving.master.ip";
  public static final String ANGEL_SERVING_MASTER_PORT = "angel.serving.master.port";
  public static final String ANGEL_SERVING_MODEL_NAME = "angel.serving.model.name";
  public static final String ANGEL_SERVING_MODEL_LOAD_TIMEOUT_MINUTE =
      "angel.serving.model.load.timeout.minute";
  public static final String ANGEL_SERVING_MODEL_LOAD_CHECK_INTEVAL_SECOND =
      "angel.serving.model.load.check.inteval.second";
  public static final String ANGEL_SERVING_MODEL_LOAD_TYPE = "angel.serving.model.load.type";
  public static final String ANGEL_SERVING_PREDICT_LOCAL_OUTPUT =
      "angel.serving.predict.local.output";

  /**
   * parameters for slow ps check
   */
  public static final String ANGEL_PS_SLOWCHECK_ENABLE = "angel.ps.slowcheck.enable";
  public static final boolean DEFAULT_ANGEL_PS_SLOWCHECK_ENABLE = false;
  public static final String ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_COUNT = "angel.ps.total.running" +
          ".rpc.exceeded.count";
  public static final int DEFAULT_ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_COUNT = 40;
  public static final String ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_THRESHOLD = "angel.ps.total" +
          ".running.rpc.exceeded.threshold";
  public static final int DEFAULT_ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_THRESHOLD = 200;
  public static final String ANGEL_PS_TOTAL_SUCCESSED_RPC_AVERAGECOSTTIME_EXCEEDED_THRESHOLD = "angel.ps.total" +
          ".successed.rpc.averagecosttime.exceeded.threshold";
  public static final int DEFAULT_ANGEL_PS_TOTAL_SUCCESSED_RPC_AVERAGECOSTTIME_EXCEEDED_THRESHOLD = 5000;

  // //////////////////////////////////////
  // Angel Metrics Configs for HTTP Post.
  // //////////////////////////////////////
  public static final String ANGEL_METRICS_ENABLE = "angel.metrics.enable";
  public static final boolean DEFAULT_ANGEL_METRICS_ENABLE = false;
  public static final String ANGEL_METRICS_HTTP_ISLOCAL_VISIT = "angel.metrics.http.islocal.visit";
  public static final boolean DEFAULT_ANGEL_METRICS_HTTP_ISLOCAL_VISIT = true;
  public static final String ANGEL_METRICS_HTTP_IP = "angel.metrics.http.ip";
  public static final String DEFAULT_ANGEL_METRICS_HTTP_IP = "127.0.0.1";
  public static final String ANGEL_METRICS_HTTP_PORT = "angel.metrics.http.port";
  public static final int DEFAULT_ANGEL_METRICS_HTTP_PORT = 8099;
  public static final String ANGEL_METRICS_HTTP_NET_TAG = "angel.metrics.http.net.tag";
  public static final String DEFAULT_ANGEL_METRICS_HTTP_NET_TAG = "all"; //all or cccc_http
  public static final String ANGEL_METRICS_HTTP_SEND_MESSAGE_TIMEOUT = "angel.metrics.http.send.message.timeout";
  public static final int DEFAULT_ANGEL_METRICS_HTTP_SEND_MESSAGE_TIMEOUT = 1000;
  // bid : business id
  public static final String ANGEL_METRICS_BID = "angel.metrics.bid";
  public static final String DEFAULT_ANGEL_METRICS_BID = "";
  // tid : interface_name
  public static final String ANGEL_METRICS_TID_ANGEL_APP = "angel.metrics.tid.angel.app";
  public static final String DEFAULT_ANGEL_METRICS_TID_ANGEL_APP = "angel_app";
  public static final String ANGEL_METRICS_TID_ANGEL_PS = "angel.metrics.tid.angel.ps";
  public static final String DEFAULT_ANGEL_METRICS_TID_ANGEL_PS = "angel_ps";
  public static final String ANGEL_METRICS_TID_ANGEL_WORKER = "angel.metrics.tid.angel.worker";
  public static final String DEFAULT_ANGEL_METRICS_TID_ANGEL_WORKER = "angel_worker";
  public static final String ANGEL_METRICS_TID_ANGEL_PS_LOAD = "angel.metrics.tid.angel.ps.load";
  public static final String DEFAULT_ANGEL_METRICS_TID_ANGEL_PS_LOAD = "angel_ps_load";

  public static final String ANGEL_METRICS_USE_JAVA_AGENT = "angel.metrics.use.java.agent";
  public static final boolean DEFAULT_ANGEL_METRICS_USE_JAVA_AGENT = false;
  public static final String ANGEL_METRICS_JAVA_AGENT_PATH = "angel.metrics.java.agent.path";
  public static final String DEFAULT_ANGEL_METRICS_JAVA_AGENT_PATH = "metricAgent-1.0-RELEASE.jar";
  //metrics separated by delimiter
  public static final String ANGEL_METRICS_DELIMITER = "angel.metrics.delimiter";
  public static final String DEFAULT_ANGEL_METRICS_DELIMITER = "|";
  //AngelMetricsReporter
  public static final String ANGEL_METRICS_REPORTER = "angel.metrics.reporter";
  public static final String DEFAULT_ANGEL_METRICS_REPORTER = "com.tencent.angel.metrics.DummyMetricsReporter";

  public static final String ANGEL_METRICS_INTERVAL_PERIOD_MILLISECOND = "angel.metrics.interval.period.millisecond";
  public static final long DEFAULT_ANGEL_METRICS_INTERVAL_PERIOD_MILLISECOND = 300000L;

  //period for ps load message report
  public static final String ANGEL_METRICS_PS_LOAD_INTERVAL_PERIOD_MILLISECOND = "angel.metrics.ps.load.interval.period.millisecond";
  public static final int DEFAULT_ANGEL_METRICS_PS_LOAD_INTERVAL_PERIOD_MILLISECOND = 60000;


  // //////////////////////////////////////
  // Angel PS resource metrics (ads) Configs.
  // //////////////////////////////////////

  /**
   * Whether to start the PS resource metrics reporter thread in each Parameter Server.
   * Disabled by default. When enabled, {@link #ANGEL_METRICS_ADS_REPORTER} must point to an
   * implementation of {@link com.tencent.angel.metrics.AngelMetricsDimensionReporter} that is
   * available on the classpath; otherwise nothing is reported.
   */
  public static final String ANGEL_METRICS_ADS_ENABLE = "angel.metrics.ads.enable";
  public static final boolean DEFAULT_ANGEL_METRICS_ADS_ENABLE = false;

  /**
   * Class name of the PS resource metrics reporter, a subclass of
   * {@link com.tencent.angel.metrics.AngelMetricsDimensionReporter}. Uses its own key so it does
   * not clash with {@link #ANGEL_METRICS_REPORTER}.
   */
  public static final String ANGEL_METRICS_ADS_REPORTER = "angel.metrics.ads.reporter";
  public static final String DEFAULT_ANGEL_METRICS_ADS_REPORTER = "";

  public static final String ANGEL_METRICS_ADS_APP_MASK = "angel.metrics.ads.app.mask";
  public static final String DEFAULT_ANGEL_METRICS_ADS_APP_MASK = "";

  public static final String ANGEL_METRICS_ADS_METRIC_GROUP = "angel.metrics.ads.metric.group";
  public static final String DEFAULT_ANGEL_METRICS_ADS_METRIC_GROUP = "";

  public static final String ANGEL_METRICS_ADS_MODEL_NAME = "angel.metrics.ads.model.name";
  public static final String DEFAULT_ANGEL_METRICS_ADS_MODEL_NAME = "";

  public static final String ANGEL_METRICS_ADS_INTERVAL_PERIOD_MILLISECOND = "angel.metrics.ads.interval.period.millisecond";
  public static final long DEFAULT_ANGEL_ADS_METRICS_INTERVAL_PERIOD_MILLISECOND = 300000L;


  // //////////////////////////////////////
  // Angel Embedding Configs.
  // //////////////////////////////////////

  public static final String ANGEL_EMBEDDING_KEYVALUE_SEP = ANGEL_PREFIX + "embedding.keyvalue.sep";
  public static final String DEFAULT_ANGEL_EMBEDDING_KEYVALUE_SEP = "colon";

  public static final String ANGEL_EMBEDDING_FEATURE_SEP = ANGEL_PREFIX + "angel.embedding.feature.sep";
  public static final String DEFAULT_ANGEL_EMBEDDING_FEATURE_SEP = "space";

  public static final String ANGEL_EMBEDDING_NORMALIZED = ANGEL_PREFIX + "embedding.normalized";
  public static final boolean DEFAULT_ANGEL_EMBEDDING_NORMALIZED = true;

  public static final String ANGEL_EMBEDDING_OPTIMIZER = ANGEL_PREFIX + "embedding.optimizer";
  public static final String DEFAULT_ANGEL_EMBEDDING_OPTIMIZER = "adam";

  public static final String ANGEL_EMBEDDING_EXPIRED_SECONDS = ANGEL_PREFIX + "embedding.expired.seconds";
  public static final long DEFAULT_ANGEL_EMBEDDING_EXPIRED_SECONDS = -1L;

  public static final String ANGEL_EMBEDDING_MODEL_WITH_CHECK_FILE = ANGEL_PREFIX + "embedding.model.with.check.file";
  public static final boolean DEFAULT_ANGEL_EMBEDDING_MODEL_WITH_CHECK_FILE = false;

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

  public class ANGEL_MODEL_PARTITIONER_DEFAULT_PARTITION_SIZE {

  }
}
