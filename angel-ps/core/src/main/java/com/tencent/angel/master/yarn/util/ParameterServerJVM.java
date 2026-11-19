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
import com.tencent.angel.metrics.MetricsUtils;
import com.tencent.angel.ps.PSAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Ps JVM command utils
 */
public class ParameterServerJVM {
  private static final Log LOG = LogFactory.getLog(ParameterServerJVM.class);

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + filter.toString();
  }

  private static String getChildEnv(Configuration jobConf) {

    return jobConf.get(AngelConf.ANGEL_PS_ENV, AngelConf.DEFAULT_ANGEL_PS_ENV);
  }

  private static String getChildLogLevel(Configuration conf) {
    return conf.get(AngelConf.ANGEL_PS_LOG_LEVEL, AngelConf.DEFAULT_ANGEL_PS_LOG_LEVEL);
  }

  /**
   * Set environment variables of ps attempt process
   *
   * @param environment environment variables of ps attempt process
   * @param conf        application configuration
   */
  public static void setVMEnv(Map<String, String> environment, Configuration conf) {
    // Add the env variables passed by the user
    String setEnv = getChildEnv(conf);
    try {
      Apps.setEnvFromInputString(environment, setEnv);
    } catch (Exception x) {
      LOG.error("set ps env faile.", x);
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
    PSAttemptId psAttemptId) {
    String userOpts;
    userOpts = jobConf.get(AngelConf.ANGEL_PS_JAVA_OPTS);

    // Old parameter name
    if(userOpts == null) {
      userOpts = jobConf.get("angel.ps.child.opts");
    }

    if (userOpts == null) {
      return generateDefaultJVMParameters(jobConf, appid, psAttemptId);
    }

    // When the user opted in to append-mode, prepend the auto-generated defaults so that
    // heap sizing, MaxDirectMemorySize, GC algorithm selection, the G1 STW-pause hardening
    // flags and -Xloggc:... are preserved. The user-supplied opts come last, which lets the
    // JVM's "last flag wins" rule make user overrides authoritative for any specific knob
    // they care about. See AngelConf#ANGEL_PS_JAVA_OPTS_APPEND_MODE javadoc.
    boolean appendMode = jobConf.getBoolean(AngelConf.ANGEL_PS_JAVA_OPTS_APPEND_MODE,
        AngelConf.DEFAULT_ANGEL_PS_JAVA_OPTS_APPEND_MODE);
    if (appendMode) {
      String defaults = generateDefaultJVMParameters(jobConf, appid, psAttemptId);
      LOG.info("PS java.opts append-mode enabled: defaults + user opts ('" + userOpts + "')");
      return defaults + " " + userOpts;
    }

    LOG.info("PS java.opts override-mode (default): user opts fully replace generated defaults."
        + " User must specify -Xmx / -XX:MaxDirectMemorySize / GC / -Xloggc themselves.");
    return userOpts;
  }

  private static String generateDefaultJVMParameters(Configuration conf, ApplicationId appid,
    PSAttemptId psAttemptId) {
    int psMemSizeInMB =
      conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB, AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB) * 1024;

    if (psMemSizeInMB < 2048) {
      psMemSizeInMB = 2048;
    }

    boolean isUseDirect = conf
      .getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);

    float directFatorUseDirectBuff = conf
      .getFloat(AngelConf.ANGEL_PS_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF,
        AngelConf.DEFAULT_ANGEL_PS_JVM_DIRECT_FACTOR_USE_DIRECT_BUFF);

    float directFatorUseHeapBuff = conf.getFloat(AngelConf.ANGEL_PS_JVM_DIRECT_FACTOR_USE_HEAP_BUFF,
      AngelConf.DEFAULT_ANGEL_PS_JVM_DIRECT_FACTOR_USE_HEAP_BUFF);

    float youngFator = conf
      .getFloat(AngelConf.ANGEL_PS_JVM_YOUNG_FACTOR, AngelConf.DEFAULT_ANGEL_PS_JVM_YOUNG_FACTOR);

    // Parallel GC parameters
    boolean isUseParallel = conf.getBoolean(AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC,
        AngelConf.DEFAULT_ANGEL_PS_JVM_USE_PARALLEL_GC);

    int parallelGCThread = conf.getInt(AngelConf.ANGEL_PS_JVM_PARALLEL_GC_THREADS,
        AngelConf.DEFAULT_ANGEL_PS_JVM_PARALLEL_GC_THREADS);

    int parallelGCMaxPauseMillis = conf.getInt(AngelConf.ANGEL_PS_JVM_PARALLEL_GC_MAX_PAUSE_TIME_MS,
        AngelConf.DEFAULT_ANGEL_PS_JVM_PARALLEL_GC_MAX_PAUSE_TIME_MS);

    boolean useAdaptiveSize = conf.getBoolean(AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE,
        AngelConf.DEFAULT_ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE);

    // G1 params
    boolean useG1 = conf
        .getBoolean(AngelConf.ANGEL_PS_JVM_USE_G1, AngelConf.DEFAULT_ANGEL_PS_JVM_USE_G1);

    int maxPauseTimeTs = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_MAXPAUSETIME_MS,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_MAXPAUSETIME_MS);

    int minNewRatio = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_MIN_NEWRATIO,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_MIN_NEWRATIO);

    int maxNewRatio = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_MAX_NEWRATIO,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_MAX_NEWRATIO);

    int regionSizeMB = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_REGIONSIZE_MB,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_REGIONSIZE_MB);

    int ihop = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_IHOP, AngelConf.DEFAULT_ANGEL_PS_JVM_G1_IHOP);

    int workerNum = conf
        .getInt(AngelConf.ANGEL_PS_JVM_G1_WORKER_NUM, AngelConf.DEFAULT_ANGEL_PS_JVM_G1_WORKER_NUM);

    int concWorkerNum = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_CONC_WORKER_NUM,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_CONC_WORKER_NUM);

    int reservePercent = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_RESERVE_PERCENT,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_RESERVE_PERCENT);

    int mixGcLiveThreshold = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_MIXGC_LIVE_THRESHOLD_PERCENT);

    int mixGcTargetCount = conf.getInt(AngelConf.ANGEL_PS_JVM_G1_MIXGC_TARGET_COUNT,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_MIXGC_TARGET_COUNT);

    // G1 STW-pause hardening flags. See AngelConf javadoc for the production rationale; in
    // short, these defeat the multi-second STW pauses caused by run-time heap expansion
    // (each expansion mmap+zero-fills several GB inside an STW pause).
    boolean pinHeap = conf.getBoolean(AngelConf.ANGEL_PS_JVM_G1_PIN_HEAP_SIZE,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_PIN_HEAP_SIZE);

    boolean alwaysPreTouch = conf.getBoolean(AngelConf.ANGEL_PS_JVM_G1_ALWAYS_PRE_TOUCH,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_ALWAYS_PRE_TOUCH);

    boolean parallelRefProc = conf.getBoolean(AngelConf.ANGEL_PS_JVM_G1_PARALLEL_REF_PROC,
        AngelConf.DEFAULT_ANGEL_PS_JVM_G1_PARALLEL_REF_PROC);

    // Carve out non-heap overhead (Metaspace, code cache, thread stacks, native libs,
    // direct-buffer headroom not pre-reserved by -XX:MaxDirectMemorySize, JVM internals)
    // BEFORE we hand the rest to the heap + direct buffers. The historical hard-coded 512
    // was the single biggest contributor to "Container killed on request, exit 137" events
    // on PSes running heavy native + Netty stacks; see ANGEL_PS_JVM_OVERHEAD_MB javadoc.
    int overheadMB = conf.getInt(AngelConf.ANGEL_PS_JVM_OVERHEAD_MB,
        AngelConf.DEFAULT_ANGEL_PS_JVM_OVERHEAD_MB);

    if (overheadMB >= psMemSizeInMB) {
      LOG.warn("PS jvm overhead (" + overheadMB + "MB) >= container memory (" + psMemSizeInMB
          + "MB); falling back to 10% of container memory to avoid a non-positive heap.");
      overheadMB = Math.max(256, psMemSizeInMB / 10);
    }

    int useMax = psMemSizeInMB - overheadMB;
    int directRegionSize = 0;
    if (isUseDirect) {
      directRegionSize = (int) (useMax * directFatorUseDirectBuff);
    } else {
      directRegionSize = (int) (useMax * directFatorUseHeapBuff);
    }

    int heapMax = useMax - directRegionSize;

    // Always log the resolved memory budget. Without this it is essentially impossible to
    // diagnose container-OOM events (exit 137) post-hoc — the operator has no way to learn
    // what -Xmx the PS launched with versus the YARN container size.
    LOG.info("PS memory budget: container=" + psMemSizeInMB + "MB, jvm-overhead="
        + overheadMB + "MB, direct=" + directRegionSize + "MB, heap=" + heapMax + "MB");
    int youngRegionSize = (int) (heapMax * youngFator);
    int survivorRatio = 4;

    String ret;

    if(isUseParallel) {
      // Parallel Scavenge + Parallel Old
      StringBuilder sb = new StringBuilder();
      sb.append(" -Xmx").append(heapMax).append("M");
      if(useAdaptiveSize) {
        sb.append(" -XX:+UseAdaptiveSizePolicy");
        sb.append(" -XX:MaxGCPauseMillis=").append(parallelGCMaxPauseMillis);
      } else {
        sb.append(" -Xms").append(heapMax).append("M");
        sb.append(" -XX:-UseAdaptiveSizePolicy");
        sb.append(" -Xmn").append(youngRegionSize).append("M");
        sb.append(" -XX:SurvivorRatio=").append(survivorRatio);
      }

      ret = sb.append(" -XX:MaxDirectMemorySize=").append(directRegionSize).append("M")
          .append(" -XX:PermSize=100M -XX:MaxPermSize=200M")
          .append(" -XX:+AggressiveOpts")
          .append(" -XX:+UseLargePages")
          .append(" -XX:+UseParallelOldGC")
          .append(" -XX:ParallelGCThreads=").append(parallelGCThread)
          .append(" -verbose:gc")
          .append(" -XX:+PrintGCDateStamps")
          .append(" -XX:+PrintGCDetails")
          .append(" -Xloggc:<LOG_DIR>/gc.log").toString();
    } else if(useG1) {
      // G1. The flag block is incrementally built so that the STW-pause hardening flags
      // (-Xms, +AlwaysPreTouch, +ParallelRefProcEnabled) can be toggled per-deployment via
      // configuration without forking the command-line shape. Each flag has a dedicated
      // AngelConf entry whose javadoc documents the production rationale.
      StringBuilder sb = new StringBuilder()
          .append(" -Xmx").append(heapMax).append("M");
      if (pinHeap) {
        // Pin Xms to Xmx so G1 never grows the committed heap at run time. Each run-time
        // expansion is an STW mmap + zero-fill of several GB; eliminating those is the
        // single biggest pause-distribution win in our deployments.
        sb.append(" -Xms").append(heapMax).append("M");
      }
      sb.append(" -XX:MaxDirectMemorySize=").append(directRegionSize).append("M")
          .append(" -XX:+AggressiveOpts")
          .append(" -XX:+UseLargePages")
          .append(" -XX:+UseG1GC")
          .append(" -XX:+UnlockExperimentalVMOptions")
          .append(" -XX:MaxGCPauseMillis=").append(maxPauseTimeTs)
          //.append(" -XX:G1NewSizePercent=").append(minNewRatio)
          //.append(" -XX:G1MaxNewSizePercent=").append(maxNewRatio)
          .append(" -XX:G1HeapRegionSize=").append(regionSizeMB).append("m")
          .append(" -XX:InitiatingHeapOccupancyPercent=").append(ihop)
          .append(" -XX:G1MixedGCLiveThresholdPercent=").append(mixGcLiveThreshold)
          .append(" -XX:G1MixedGCCountTarget=").append(mixGcTargetCount)
          .append(" -XX:ConcGCThreads=").append(concWorkerNum)
          .append(" -XX:ParallelGCThreads=").append(workerNum)
          .append(" -XX:G1ReservePercent=").append(reservePercent);
      if (alwaysPreTouch) {
        // Touch all heap pages at startup. Combined with -Xms == -Xmx this makes the heap
        // truly hot from t=0 and is what removes the [Other: 1496.7 ms] component caused
        // by a kernel-level commit + zero from the steady-state pause path.
        sb.append(" -XX:+AlwaysPreTouch");
      }
      if (parallelRefProc) {
        sb.append(" -XX:+ParallelRefProcEnabled");
      }
      ret = sb.append(" -verbose:gc")
          .append(" -XX:+PrintGCDateStamps").append(" -XX:+PrintGCDetails")
          .append(" -Xloggc:<LOG_DIR>/gc.log").toString();
    } else {
      // CMS
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

    LOG.info("PS GC parameters: " + ret);
    return ret;
  }

  private static void setupLog4jProperties(Configuration conf, Vector<String> vargs, long logSize) {
    String logLevel = getChildLogLevel(conf);
    AngelApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  /**
   * Generate ps attempt jvm command
   *
   * @param conf        application configuration
   * @param appid       application id
   * @param psAttemptId ps attempt id
   * @return ps attempt jvm command
   */
  public static List<String> getVMCommand(Configuration conf, ApplicationId appid,
    PSAttemptId psAttemptId, ContainerId containerId) {

    Vector<String> vargs = new Vector<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    String javaOpts = getChildJavaOpts(conf, appid, psAttemptId);
    String[] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    Path childTmpDir = new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);

    // Setup the log4j prop
    long logSize = 0;// TaskLog.getTaskLogLength(conf);
    setupLog4jProperties(conf, vargs, logSize);

    //use agent to collect ps containerId, help jvm get jvm_task_id
    if (conf.getBoolean(AngelConf.ANGEL_METRICS_USE_JAVA_AGENT, AngelConf.DEFAULT_ANGEL_METRICS_USE_JAVA_AGENT) &&
        conf.getBoolean(AngelConf.ANGEL_METRICS_ENABLE, AngelConf.DEFAULT_ANGEL_METRICS_ENABLE)) {
      vargs.add("-javaagent:" +
          conf.get(AngelConf.ANGEL_METRICS_JAVA_AGENT_PATH, AngelConf.DEFAULT_ANGEL_METRICS_JAVA_AGENT_PATH) +
          "=jvm_task_id=" + containerId.toString() + ":product=angel");
    }

    // Add main class and its arguments
    String psClassName = conf.get(AngelConf.ANGEL_PS_CLASS, AngelConf.DEFAULT_ANGEL_PS_CLASS);
    vargs.add(psClassName); // main of Child

    // Finally add the jvmID
    // vargs.add(String.valueOf(jvmID.getId()));
    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());

    LOG.info("Command to launch container for PS is : " + mergedCommand);

    return vargsFinal;
  }
}
