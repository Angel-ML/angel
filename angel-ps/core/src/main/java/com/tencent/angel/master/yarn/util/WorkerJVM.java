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


package com.tencent.angel.master.yarn.util;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Worker JVM command utils
 */
public class WorkerJVM {
  private static final Log LOG = LogFactory.getLog(WorkerJVM.class);

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + filter.toString();
  }

  private static String getChildEnv(Configuration jobConf) {

    return jobConf.get(AngelConf.ANGEL_WORKER_ENV, AngelConf.DEFAULT_ANGEL_WORK_ENV);
  }

  private static String getChildLogLevel(Configuration conf) {
    return conf.get(AngelConf.ANGEL_WORKER_LOG_LEVEL, AngelConf.DEFAULT_ANGEL_WORKER_LOG_LEVEL);
  }

  private static String generateDefaultJVMParameters(Configuration conf, ApplicationId appid,
    WorkerAttemptId workerAttemptId) {
    int workerMemSizeInMB =
      conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB, AngelConf.DEFAULT_ANGEL_WORKER_MEMORY_GB)
        * 1024;

    if (workerMemSizeInMB < 2048) {
      workerMemSizeInMB = 2048;
    }

    boolean isUseDirect = conf
      .getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER);

    float directFatorUseDirectBuff = conf
      .getFloat(AngelConf.ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF);

    float directFatorUseHeapBuff = conf.getFloat(AngelConf.ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_HEAP_BUFF,
      AngelConf.DEFAULT_ANGEL_WORKER_JVM_DIRECT_FACTOR_USE_HEAP_BUFF);

    float youngFator = conf
      .getFloat(AngelConf.ANGEL_WORKER_JVM_YOUNG_FACTOR, AngelConf.DEFAULT_ANGEL_WORKER_JVM_YOUNG_FACTOR);

    // G1 params
    boolean useG1 = conf
        .getBoolean(AngelConf.ANGEL_WORKER_JVM_USE_G1, AngelConf.DEFAULT_ANGEL_WORKER_JVM_USE_G1);

    int maxPauseTimeTs = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_MAXPAUSETIME_MS,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_MAXPAUSETIME_MS);

    int regionSizeMB = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_REGIONSIZE_MB,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_REGIONSIZE_MB);

    int ihop = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_IHOP, AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_IHOP);

    int workerNum = conf
        .getInt(AngelConf.ANGEL_WORKER_JVM_G1_WORKER_NUM, AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_WORKER_NUM);

    int concWorkerNum = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_CONC_WORKER_NUM,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_CONC_WORKER_NUM);

    int reservePercent = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_RESERVE_PERCENT,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_RESERVE_PERCENT);

    int mixGcLiveThreshold = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT);

    int mixGcTargetCount = conf.getInt(AngelConf.ANGEL_WORKER_JVM_G1_MIXGC_TARGET_COUNT,
        AngelConf.DEFAULT_ANGEL_WORKER_JVM_G1_MIXGC_TARGET_COUNT);

    int maxUse = workerMemSizeInMB - 512;
    int directRegionSize = 0;
    if (isUseDirect) {
      directRegionSize = (int) (maxUse * directFatorUseDirectBuff);
    } else {
      directRegionSize = (int) (maxUse * directFatorUseHeapBuff);
    }
    int heapMax = maxUse - directRegionSize;
    int youngRegionSize = (int) (heapMax * youngFator);
    int survivorRatio = 4;

    String ret;
    if(useG1) {
      ret = new StringBuilder().append(" -Xmx").append(heapMax).append("M")
          .append(" -XX:MaxDirectMemorySize=")
          .append(directRegionSize).append("M")
          .append(" -XX:+AggressiveOpts")
          .append(" -XX:+UseLargePages")
          .append(" -XX:+UseG1GC")
          .append(" -XX:+UnlockExperimentalVMOptions")
          .append(" -XX:MaxGCPauseMillis=").append(maxPauseTimeTs)
          .append(" -XX:G1HeapRegionSize=").append(regionSizeMB).append("m")
          .append(" -XX:InitiatingHeapOccupancyPercent=").append(ihop)
          .append(" -XX:G1MixedGCLiveThresholdPercent=").append(mixGcLiveThreshold)
          .append(" -XX:G1MixedGCCountTarget=").append(mixGcTargetCount)
          .append(" -XX:ConcGCThreads=").append(concWorkerNum)
          .append(" -XX:ParallelGCThreads=").append(workerNum)
          .append(" -XX:G1ReservePercent=").append(reservePercent)
          .append(" -verbose:gc")
          .append(" -XX:+PrintGCDateStamps").append(" -XX:+PrintGCDetails")
          .append(" -Xloggc:<LOG_DIR>/gc.log").toString();
    } else {
      ret = new StringBuilder().append(" -Xmx").append(heapMax).append("M").append(" -Xmn")
          .append(youngRegionSize).append("M").append(" -XX:MaxDirectMemorySize=")
          .append(directRegionSize).append("M").append(" -XX:SurvivorRatio=").append(survivorRatio)
          .append(" -XX:PermSize=100M -XX:MaxPermSize=200M").append(" -XX:+AggressiveOpts")
          .append(" -XX:+UseLargePages").append(" -XX:+UseConcMarkSweepGC")
          .append(" -XX:CMSInitiatingOccupancyFraction=70")
          .append(" -XX:+UseCMSInitiatingOccupancyOnly").append(" -XX:+CMSScavengeBeforeRemark")
          .append(" -XX:+UseCMSCompactAtFullCollection").append(" -verbose:gc")
          .append(" -XX:+PrintGCDateStamps").append(" -XX:+PrintGCDetails")
          .append(" -Xloggc:<LOG_DIR>/gc.log").toString();
    }

    return ret;
  }

  /**
   * Set environment variables of worker attempt process
   *
   * @param environment environment variables of ps attempt process
   * @param conf        application configuration
   */
  public static void setVMEnv(Map<String, String> environment, Configuration conf) {
    // Add the env variables passed by the user
    String workerChildEnv = getChildEnv(conf);
    LOG.info("worker env=" + workerChildEnv);
    try {
      Apps.setEnvFromInputString(environment, workerChildEnv);
    } catch (Exception x) {
      LOG.error("set worker env failed.", x);
    }

    // Set logging level in the environment.
    environment.put("HADOOP_ROOT_LOGGER", getChildLogLevel(conf) + ",CLA");

    String hadoopClientOpts = System.getenv("HADOOP_CLIENT_OPTS");
    if (hadoopClientOpts == null) {
      hadoopClientOpts = "";
    } else {
      hadoopClientOpts = hadoopClientOpts + " ";
    }

    long logSize = 0;
    Vector<String> logProps = new Vector<String>(4);
    setupLog4jProperties(conf, logProps, logSize);
    Iterator<String> it = logProps.iterator();
    StringBuilder buffer = new StringBuilder();
    while (it.hasNext()) {
      buffer.append(" ").append(it.next());
    }
    hadoopClientOpts = hadoopClientOpts + buffer.toString();
    environment.put("HADOOP_CLIENT_OPTS", hadoopClientOpts);
  }

  private static String getChildJavaOpts(Configuration jobConf, ApplicationId appid,
    WorkerAttemptId workerAttemptId) {
    String userOpts = null;
    userOpts = jobConf.get(AngelConf.ANGEL_WORKER_JAVA_OPTS);
    if (userOpts == null) {
      userOpts = generateDefaultJVMParameters(jobConf, appid, workerAttemptId);
    }

    return userOpts;
  }

  private static void setupLog4jProperties(Configuration conf, Vector<String> vargs, long logSize) {
    String logLevel = getChildLogLevel(conf);
    AngelApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  /**
   * Create worker attempt jvm command
   *
   * @param conf            application configuration
   * @param appid           application id
   * @param workerAttemptId worker attempt id
   * @return
   */
  public static List<String> getVMCommand(Configuration conf, ApplicationId appid,
    WorkerAttemptId workerAttemptId) {
    Vector<String> vargs = new Vector<String>(8);

    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    String javaOpts = getChildJavaOpts(conf, appid, workerAttemptId);
    LOG.debug("javaOpts=" + javaOpts);

    String[] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    Path childTmpDir = new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);

    // Setup the log4j prop
    long logSize = 0;
    setupLog4jProperties(conf, vargs, logSize);

    // Add main class and its arguments
    String workerClassName =
      conf.get(AngelConf.ANGEL_WORKER_CLASS, AngelConf.DEFAULT_ANGEL_WORKER_CLASS);
    vargs.add(workerClassName);

    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }
}
