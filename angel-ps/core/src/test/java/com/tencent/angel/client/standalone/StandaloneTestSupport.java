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

package com.tencent.angel.client.standalone;

import com.tencent.angel.RunningMode;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.utils.AngelPSClusterMain;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

/**
 * Shared helpers for the STANDALONE / cluster end-to-end matrix tests.
 *
 * <p>All tests built on top of this class spawn real Parameter Server JVMs and are therefore
 * gated by {@code -Dangel.test.standalone.enabled=true}, exactly like {@link StandaloneAppTest}.
 */
final class StandaloneTestSupport {

  private static final Log LOG = LogFactory.getLog(StandaloneTestSupport.class);

  static final String ENABLE_PROPERTY = "angel.test.standalone.enabled";
  static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  private StandaloneTestSupport() {
  }

  static boolean enabled() {
    return Boolean.getBoolean(ENABLE_PROPERTY);
  }

  /** Create (and clean) a unique scratch directory for one test class. */
  static File newRunDir(String prefix) {
    File dir = new File(TMP_PATH, prefix + "-" + System.currentTimeMillis());
    FileUtil.fullyDelete(dir);
    if (!dir.mkdirs()) {
      throw new IllegalStateException("can not create " + dir);
    }
    return dir;
  }

  static void deleteQuietly(File dir) {
    if (dir != null) {
      FileUtil.fullyDelete(dir);
    }
  }

  /**
   * Configuration for a PS-only STANDALONE cluster on the local host: the master runs in the
   * calling JVM and every PS is a child JVM spawned by {@code StandaloneContainerLauncher}.
   */
  static Configuration psOnlyConf(File runDir, int psNum) {
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString());
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());

    String root = runDir.getAbsolutePath();
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + root + "/owner-out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + root + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + root + "/log");
    conf.set(AngelConf.ANGEL_STAGING_DIR, LOCAL_FS + root + "/staging");
    conf.set(AngelConf.ANGEL_STANDALONE_LOG_DIR, root + "/standalone-logs");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 0);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, psNum);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);
    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, "-Xmx512m");
    return conf;
  }

  /**
   * Start a PSAgent bound to {@code master}, i.e. the same data-plane client a Spark executor
   * builds in {@code AngelPSContext}. Only one PSAgent may live in a JVM (PSAgentContext is a
   * singleton), so callers must {@link PSAgent#stop()} it before starting another one.
   */
  static PSAgent startPSAgent(Configuration conf, Location master) throws Exception {
    PSAgent agent = new PSAgent(conf, master.getIp(), master.getPort(), 0, false, null);
    agent.initAndStart();
    return agent;
  }

  /**
   * Handle of a long-running PS cluster brought up by {@link AngelPSClusterMain} in a separate
   * JVM, i.e. what {@code bin/angel-cluster-up} does on the AM host.
   */
  static final class ExternalCluster {
    final Process process;
    final Location master;
    final File log;

    ExternalCluster(Process process, Location master, File log) {
      this.process = process;
      this.master = master;
      this.log = log;
    }

    void stop() {
      if (process == null) {
        return;
      }
      // SIGTERM -> AngelPSClusterMain shutdown hook -> AngelClient.stop() tears the PSes down.
      process.destroy();
      try {
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
          LOG.warn("cluster process did not exit in 60s, killing it");
          process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }
  }

  /**
   * Launch {@link AngelPSClusterMain} as a child JVM and wait until it publishes the master
   * address through {@code angel.master.location.file}.
   */
  static ExternalCluster launchCluster(File runDir, int psNum, long timeoutMs)
      throws IOException, InterruptedException {
    File locFile = new File(runDir, "master.location");
    File log = new File(runDir, "angel-ps-cluster.log");
    String root = runDir.getAbsolutePath();

    // ANGEL_HOME must contain conf/angel-site.xml; surefire runs inside angel-ps/core.
    File angelHome = new File("..").getCanonicalFile();
    File log4j = new File(angelHome, "conf/log4j.properties");

    String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
        + "java";
    List<String> cmd = new ArrayList<>();
    cmd.add(javaBin);
    cmd.add("-Xmx512m");
    if (log4j.exists()) {
      cmd.add("-Dlog4j.configuration=file:" + log4j.getAbsolutePath());
    }
    cmd.add("-cp");
    cmd.add(System.getProperty("java.class.path"));
    cmd.add(AngelPSClusterMain.class.getName());
    cmd.add("-D" + AngelConf.ANGEL_DEPLOY_MODE + "=STANDALONE");
    cmd.add("-D" + AngelConf.ANGEL_PS_NUMBER + "=" + psNum);
    cmd.add("-D" + AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS + "=1000");
    cmd.add("-D" + AngelConf.ANGEL_PS_JAVA_OPTS + "=-Xmx512m");
    cmd.add("-D" + AngelConf.ANGEL_SAVE_MODEL_PATH + "=" + LOCAL_FS + root + "/cluster-out");
    cmd.add("-D" + AngelConf.ANGEL_LOG_PATH + "=" + LOCAL_FS + root + "/log");
    cmd.add("-D" + AngelConf.ANGEL_STAGING_DIR + "=" + LOCAL_FS + root + "/staging");
    cmd.add("-D" + AngelConf.ANGEL_STANDALONE_LOG_DIR + "=" + root + "/standalone-logs");
    cmd.add("-D" + AngelConf.ANGEL_INPUTFORMAT_CLASS + "="
        + CombineTextInputFormat.class.getName());
    cmd.add("-D" + AngelPSClusterMain.ANGEL_MASTER_LOCATION_FILE + "="
        + locFile.getAbsolutePath());

    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.environment().put("ANGEL_HOME", angelHome.getAbsolutePath());
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
    LOG.info("launching Angel PS cluster: " + String.join(" ", cmd.subList(0, 3)) + " ...");
    Process process = pb.start();

    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (!process.isAlive()) {
        throw new IllegalStateException("AngelPSClusterMain exited with code "
            + process.exitValue() + ", see " + log);
      }
      if (locFile.exists()) {
        String addr = new String(Files.readAllBytes(locFile.toPath()), StandardCharsets.UTF_8)
            .trim();
        int sep = addr.lastIndexOf(':');
        if (sep > 0) {
          Location master = new Location(addr.substring(0, sep),
              Integer.parseInt(addr.substring(sep + 1)));
          LOG.info("Angel PS cluster is up, master=" + master);
          return new ExternalCluster(process, master, log);
        }
      }
      Thread.sleep(500);
    }
    process.destroyForcibly();
    throw new IllegalStateException("AngelPSClusterMain did not publish its master address in "
        + timeoutMs + " ms, see " + log);
  }
}
