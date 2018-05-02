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

package com.tencent.angel.master.yarn.util;

import com.tencent.angel.conf.AngelConf;
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
    return conf.get(AngelConf.ANGEL_PS_LOG_LEVEL,
        AngelConf.DEFAULT_ANGEL_PS_LOG_LEVEL);
  }

  /**
   * Set environment variables of ps attempt process
   * @param environment environment variables of ps attempt process
   * @param conf application configuration
   */
  public static void setVMEnv(Map<String, String> environment, Configuration conf) {
    // Add the env variables passed by the user
    String setEnv = getChildEnv(conf);
    try{
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
    String userOpts = null;
    userOpts = jobConf.get(AngelConf.ANGEL_PS_JAVA_OPTS);
    if (userOpts == null) {
      userOpts = generateDefaultJVMParameters(jobConf, appid, psAttemptId);
    }

    return userOpts;
  }

  private static String generateDefaultJVMParameters(Configuration conf, ApplicationId appid,
      PSAttemptId psAttemptId) {
    int psMemSizeInMB =
        conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB,
            AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB) * 1024;

    if(psMemSizeInMB < 2048) {
      psMemSizeInMB = 2048;
    }

    boolean isUseDirect = conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);

    int useMax = psMemSizeInMB - 512;
    int directRegionSize = 0;
    if(isUseDirect) {
      directRegionSize = (int) (useMax * 0.45);
    } else {
      directRegionSize = (int) (useMax * 0.25);
    }

    int heapMax = useMax - directRegionSize;
    int youngRegionSize = (int) (heapMax * 0.4);
    int survivorRatio = 4;

    String ret =
        new StringBuilder().append(" -Xmx").append(heapMax).append("M").append(" -Xmn")
            .append(youngRegionSize).append("M").append(" -XX:MaxDirectMemorySize=")
            .append(directRegionSize).append("M").append(" -XX:SurvivorRatio=")
            .append(survivorRatio).append(" -XX:PermSize=100M -XX:MaxPermSize=200M")
            .append(" -XX:+AggressiveOpts").append(" -XX:+UseLargePages")
            .append(" -XX:+UseConcMarkSweepGC")
            .append(" -XX:CMSInitiatingOccupancyFraction=70")
            .append(" -XX:+UseCMSInitiatingOccupancyOnly").append(" -XX:+CMSScavengeBeforeRemark")
            .append(" -XX:+UseCMSCompactAtFullCollection").append(" -verbose:gc")
            .append(" -XX:+PrintGCDateStamps").append(" -XX:+PrintGCDetails")
            .append(" -XX:+PrintCommandLineFlags").append(" -XX:+PrintTenuringDistribution")
            .append(" -XX:+PrintAdaptiveSizePolicy").append(" -Xloggc:<LOG_DIR>/gc.log").toString();;

    return ret;
  }

  private static void setupLog4jProperties(Configuration conf, Vector<String> vargs, long logSize) {
    String logLevel = getChildLogLevel(conf);
    AngelApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  /**
   * Generate ps attempt jvm command
   * @param conf application configuration
   * @param appid application id
   * @param psAttemptId ps attempt id
   * @return ps attempt jvm command
   */
  public static List<String> getVMCommand(Configuration conf, ApplicationId appid,
      PSAttemptId psAttemptId) {

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

    // Add main class and its arguments
    String psClassName =
        conf.get(AngelConf.ANGEL_PS_CLASS, AngelConf.DEFAULT_ANGEL_PS_CLASS);
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
