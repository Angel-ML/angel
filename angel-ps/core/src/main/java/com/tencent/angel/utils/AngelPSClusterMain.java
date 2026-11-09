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

package com.tencent.angel.utils;

import com.tencent.angel.RunningMode;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Standalone entry point that brings up an Angel <b>master + parameter servers</b> cluster
 * and exposes the master rpc address to external computing engines.
 *
 * <p>Unlike {@link AngelRunJar}, this class does NOT require an {@code AppSubmitter}
 * implementation. It performs exactly these steps:
 * <ol>
 *   <li>Force {@code angel.running.mode=ANGEL_PS} so no Worker processes are scheduled.</li>
 *   <li>Create an {@link AngelClient} and call {@code startPSServer()} to bring up the
 *       in-process AM and remote PS processes (single-node or distributed STANDALONE).</li>
 *   <li>Print {@code ANGEL_MASTER=<ip>:<port>} to stdout and, optionally, persist the
 *       address to a file pointed at by {@code angel.master.location.file}. External
 *       engines can parse either.</li>
 *   <li>Block until the process receives SIGINT / SIGTERM or the user invokes
 *       {@link #shutdown()}.</li>
 * </ol>
 *
 * <p>The cluster is considered <i>long-running</i>: consumers connect via
 * {@code angel.ipc.TConnectionManager.getMasterService(ip, port)} to create matrices, run
 * RPCs against PSes, and eventually stop the cluster by killing this process.
 *
 * <p>Minimum command line:
 * <pre>
 *   java com.tencent.angel.utils.AngelPSClusterMain \
 *        -Dangel.deploy.mode=STANDALONE \
 *        -Dangel.am.host=10.0.0.1 \
 *        -Dangel.standalone.ps.hosts=10.0.0.2,10.0.0.3 \
 *        -Dangel.ps.number=2 \
 *        -Dangel.standalone.remote.classpath=/opt/angel/lib/* \
 *        -Dangel.save.model.path=file:///tmp/angel/model
 * </pre>
 */
public class AngelPSClusterMain {

  private static final Log LOG = LogFactory.getLog(AngelPSClusterMain.class);

  /**
   * Optional configuration key: if set, the {@code ip:port} of the master is written to
   * this file (atomically replaced on every start). External engines can watch this file
   * to discover the master address without parsing stdout.
   */
  public static final String ANGEL_MASTER_LOCATION_FILE = "angel.master.location.file";

  /** Latch flipped by {@link #shutdown()} / SIGINT / SIGTERM. */
  private static final CountDownLatch STOP_LATCH = new CountDownLatch(1);

  public static void main(String[] args) {
    Configuration conf;
    try {
      conf = ConfUtils.initConf(args);
    } catch (Exception e) {
      LOG.fatal("failed to initialize configuration", e);
      System.exit(2);
      return;
    }

    // PS-only: never schedule Worker containers.
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString());
    // ANGEL_PS mode does not run any worker-side tasks; keep workergroup count at 0.
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 0);

    // A pure PS cluster has no real input/output directory, but AngelClient.setInput-/
    // setOutputDirectory() still validate them. Inject sane defaults so users do NOT have
    // to specify dummy paths just to bring up a long-running PS cluster.
    applyPSOnlyPathDefaults(conf);

    try {
      UserGroupInformation.setConfiguration(conf);
    } catch (Throwable ignore) {
      // best-effort: UGI setup is not strictly required for local/standalone
    }

    AngelClient client = AngelClientFactory.get(conf);

    // Install a shutdown hook so the cluster is torn down cleanly on Ctrl-C / kill.
    final Thread mainThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread("angel-ps-cluster-shutdown") {
      @Override public void run() {
        LOG.info("Received shutdown signal, stopping Angel PS cluster ...");
        STOP_LATCH.countDown();
        try {
          client.stop();
        } catch (Throwable t) {
          LOG.warn("error while stopping AngelClient", t);
        }
        try {
          mainThread.join(30_000);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
        }
      }
    });

    try {
      client.startPSServer();
      Location loc = client.getMasterLocation();
      if (loc == null) {
        throw new IllegalStateException("AngelClient.getMasterLocation() returned null");
      }

      // Drive the master state machine into RUNNING. Without this, the App stays in
      // state NEW forever (no Worker is ever launched in ANGEL_PS-only mode and no
      // user submitter calls master.start()), which trips App.stateMonitor's
      // `angel.am.appstate.timeout.ms` (default 50 min) and then floods the log with
      // "lack of resources..." ERRORs before tearing the master down. AngelClient.run()
      // sends a StartRequest -> NEW -> INITED -> PREPARE_WORKERS -> ALL_WORKERS_LAUNCHED
      // (synthesised when running mode is ANGEL_PS) -> RUNNING. RUNNING is excluded
      // from stateMonitor's timeout map, so the cluster stays up indefinitely.
      try {
        client.run();
        LOG.info("Master moved to RUNNING via StartRequest.");
      } catch (Throwable t) {
        LOG.warn("client.run() failed; the master will stay in state NEW and may exit "
            + "after angel.am.appstate.timeout.ms. Cause: " + t.getMessage(), t);
      }

      // 1) Machine-readable line on stdout. Intentionally prefixed so log parsers can grep.
      String addr = loc.getIp() + ":" + loc.getPort();
      System.out.println("ANGEL_MASTER=" + addr);
      System.out.flush();
      LOG.info("Angel PS cluster is up. Master at " + addr);

      // 2) Optional: persist to a file consumers can poll / watch.
      String locFile = conf.get(ANGEL_MASTER_LOCATION_FILE);
      if (locFile != null && !locFile.isEmpty()) {
        writeLocationFile(new File(locFile), addr);
        LOG.info("Master address persisted to " + locFile);
      }

      // 3) External-PS mode: emit a manifest the operator can use to bring up each PS by
      // hand on whichever machine they choose. The launcher already short-circuits the
      // child-spawn path, so without this manifest the user would have no way of
      // discovering which (serverIndex, attemptIndex) pairs the master is waiting for.
      maybeWriteExternalPsManifest(client, conf, addr);

      LOG.info("Waiting for stop signal (SIGINT/SIGTERM) ...");
      STOP_LATCH.await();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      LOG.fatal("Failed to start Angel PS cluster", t);
      try {
        client.stop();
      } catch (Throwable ignore) {
      }
      System.exit(1);
    }
    LOG.info("Angel PS cluster exited.");
  }

  /** Programmatic shutdown trigger (useful when this class is embedded in another JVM). */
  public static void shutdown() {
    STOP_LATCH.countDown();
  }

  private static void writeLocationFile(File file, String addr) throws IOException {
    File parent = file.getParentFile();
    if (parent != null && !parent.exists()) {
      //noinspection ResultOfMethodCallIgnored
      parent.mkdirs();
    }
    File tmp = new File(file.getAbsolutePath() + ".tmp");
    try (OutputStream out = new FileOutputStream(tmp)) {
      out.write(addr.getBytes(StandardCharsets.UTF_8));
      out.write('\n');
    }
    if (!tmp.renameTo(file)) {
      // Windows: rename fails if target exists; fall back to delete+rename.
      //noinspection ResultOfMethodCallIgnored
      file.delete();
      if (!tmp.renameTo(file)) {
        throw new IOException("Failed to publish master address to " + file);
      }
    }
  }

  /**
   * Emit the external-PS launch manifest when the master is configured to skip
   * spawning child PS processes. Each line of the manifest is either a {@code key=value}
   * header pair, a comment ({@code #...}), or one of:
   *
   * <pre>
   * ps &lt;serverIndex&gt; &lt;attemptIndex&gt;
   * </pre>
   *
   * Operators feed the manifest path to {@code bin/angel-ps-launch} (or read it from any
   * orchestration tool) to bring up each PS by hand. The same information is also
   * mirrored on stdout so it is captured by {@code angel-cluster-up}'s log file.
   */
  private static void maybeWriteExternalPsManifest(AngelClient client, Configuration conf,
                                                   String masterAddr) {
    boolean external = conf.getBoolean(AngelConf.ANGEL_STANDALONE_PS_EXTERNAL,
        AngelConf.DEFAULT_ANGEL_STANDALONE_PS_EXTERNAL);
    if (!external) {
      return;
    }

    int psNumber = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    // Best-effort: if the embedded client is the standalone flavor, surface the local
    // job.xml path so operators know which file to deploy alongside Angel jars on every
    // PS host. Other client types (yarn/local/...) do not apply here because external-PS
    // mode only makes sense in STANDALONE.
    String jobConfFile = null;
    String appId = conf.get(AngelConf.ANGEL_EMBEDDED_APP_ID);
    try {
      if (client instanceof com.tencent.angel.client.standalone.AngelStandaloneClient) {
        jobConfFile = ((com.tencent.angel.client.standalone.AngelStandaloneClient) client)
            .getJobConfFile();
      }
    } catch (Throwable ignore) {
      // The accessor is optional; fall through with jobConfFile == null.
    }

    StringBuilder manifest = new StringBuilder();
    manifest.append("# Angel external-PS launch manifest\n");
    manifest.append("# Generated when ").append(AngelConf.ANGEL_STANDALONE_PS_EXTERNAL)
        .append("=true. Each `ps` line corresponds to one PSAttempt the master\n");
    manifest.append("# is currently waiting for. Bring it up by running, on the chosen "
        + "host:\n");
    manifest.append("#   bin/angel-ps-launch --manifest <this-file> --ps-id <serverIndex>\n");
    manifest.append("master=").append(masterAddr).append('\n');
    if (appId != null) {
      manifest.append("appId=").append(appId).append('\n');
    }
    if (jobConfFile != null) {
      manifest.append("jobConf=").append(jobConfFile).append('\n');
    }
    manifest.append("psNumber=").append(psNumber).append('\n');
    manifest.append("# ps <serverIndex> <attemptIndex>\n");
    for (int i = 0; i < psNumber; i++) {
      manifest.append("ps ").append(i).append(' ').append(0).append('\n');
    }

    // Always echo to stdout so the data ends up in angel-cluster-up's log file.
    System.out.println("===== Angel external-PS manifest =====");
    System.out.print(manifest);
    System.out.println("======================================");
    System.out.flush();

    String manifestFile = conf.get(AngelConf.ANGEL_STANDALONE_PS_MANIFEST_FILE);
    if (manifestFile == null || manifestFile.isEmpty()) {
      LOG.warn("External-PS mode is enabled but " + AngelConf.ANGEL_STANDALONE_PS_MANIFEST_FILE
          + " is unset. Manifest only printed to stdout.");
      return;
    }
    try {
      writeManifestFile(new File(manifestFile), manifest.toString());
      LOG.info("External-PS launch manifest written to " + manifestFile);
    } catch (IOException ioe) {
      LOG.error("Failed to write external-PS manifest to " + manifestFile, ioe);
    }
  }

  private static void writeManifestFile(File file, String content) throws IOException {
    File parent = file.getParentFile();
    if (parent != null && !parent.exists()) {
      //noinspection ResultOfMethodCallIgnored
      parent.mkdirs();
    }
    File tmp = new File(file.getAbsolutePath() + ".tmp");
    try (OutputStream out = new FileOutputStream(tmp)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
    if (!tmp.renameTo(file)) {
      //noinspection ResultOfMethodCallIgnored
      file.delete();
      if (!tmp.renameTo(file)) {
        throw new IOException("Failed to publish manifest to " + file);
      }
    }
  }

  /**
   * AngelClient validates the train/predict/serving input & output paths even when nothing
   * actually reads or writes them in ANGEL_PS mode. To keep the PS-only entry point
   * dependency-free, fill in safe local-FS defaults under {@code java.io.tmpdir} for any
   * path the caller did not specify.
   */
  private static void applyPSOnlyPathDefaults(Configuration conf) {
    String actionType = conf.get(AngelConf.ANGEL_ACTION_TYPE,
        AngelConf.DEFAULT_ANGEL_ACTION_TYPE);
    String tmpRoot = "file://" + System.getProperty("java.io.tmpdir", "/tmp")
        + "/angel-ps-cluster";

    // Ensure setOutputDirectory() succeeds: for "train"/"inctrain" angel.save.model.path
    // is required, for "predict" angel.predict.path, for "serving" angel.serving.temp.path.
    if ("train".equals(actionType) || "inctrain".equals(actionType)) {
      if (isBlank(conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))) {
        conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, tmpRoot + "/model");
      }
    } else if ("predict".equals(actionType)) {
      if (isBlank(conf.get(AngelConf.ANGEL_PREDICT_PATH))) {
        conf.set(AngelConf.ANGEL_PREDICT_PATH, tmpRoot + "/predict");
      }
    } else if ("serving".equals(actionType)) {
      if (isBlank(conf.get(AngelConf.ANGEL_SERVING_TEMP_PATH))) {
        conf.set(AngelConf.ANGEL_SERVING_TEMP_PATH, tmpRoot + "/serving");
      }
    } else {
      // Unknown action: default to "train" + a save path so AngelClient doesn't bail out.
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "train");
      if (isBlank(conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH))) {
        conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, tmpRoot + "/model");
      }
    }

    // Skip the input-path requirement: ANGEL_PS already short-circuits setInputDirectory(),
    // but enabling the dummy data spliter makes the behaviour explicit and decouples this
    // code from the running-mode check inside AngelClient.
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);

    // Allow the temp output dir to be overwritten across restarts of the same cluster.
    if (conf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST) == null) {
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    }

    // Sanity check the configured output path: catching "hdfs:/foo" / "hdfs:///foo" (no
    // authority) up front produces a much clearer error than the downstream
    // "Incomplete HDFS URI, no host" that comes out of
    // DistributedFileSystem.initialize() when the tmp-dir path is assembled later in
    // HdfsUtil.generateTmpDirectory().
    validateOutputPathAuthority(conf, AngelConf.ANGEL_SAVE_MODEL_PATH);
    validateOutputPathAuthority(conf, AngelConf.ANGEL_PREDICT_PATH);
    validateOutputPathAuthority(conf, AngelConf.ANGEL_SERVING_TEMP_PATH);
  }

  /**
   * Emit a loud, actionable WARN if the user-configured output path uses an HDFS-like
   * scheme but has no authority (host / nameservice) and {@code fs.defaultFS} is not a
   * matching HDFS URI either. The job would otherwise fail deep inside
   * {@link org.apache.hadoop.hdfs.DistributedFileSystem#initialize} with the unhelpful
   * message "Incomplete HDFS URI, no host".
   */
  private static void validateOutputPathAuthority(Configuration conf, String key) {
    String raw = conf.get(key);
    if (isBlank(raw)) {
      return;
    }
    java.net.URI uri;
    try {
      uri = new java.net.URI(raw);
    } catch (java.net.URISyntaxException e) {
      LOG.warn(key + "=" + raw + " is not a valid URI: " + e.getMessage());
      return;
    }
    String scheme = uri.getScheme();
    if (scheme == null || "file".equalsIgnoreCase(scheme)) {
      return;
    }
    String host = uri.getHost();
    if (host != null && !host.isEmpty()) {
      return;
    }
    // No authority in the user-supplied value. Try to see if fs.defaultFS fills it in.
    java.net.URI defaultFs;
    try {
      defaultFs = org.apache.hadoop.fs.FileSystem.getDefaultUri(conf);
    } catch (Throwable t) {
      defaultFs = null;
    }
    if (defaultFs != null && scheme.equalsIgnoreCase(defaultFs.getScheme())
        && defaultFs.getHost() != null && !defaultFs.getHost().isEmpty()) {
      LOG.info(key + "=" + raw + " has no explicit authority; will be resolved against "
          + "fs.defaultFS=" + defaultFs);
      return;
    }
    LOG.warn("================================================================");
    LOG.warn(key + "=" + raw);
    LOG.warn("  is an " + scheme + "-scheme path but has no authority (nameservice/host),");
    LOG.warn("  and fs.defaultFS is not a matching " + scheme + ":// URI either.");
    LOG.warn("  The job will fail with \"Incomplete HDFS URI, no host\" once Angel tries");
    LOG.warn("  to materialise a tmp dir under this path. To fix:");
    LOG.warn("    1) export HADOOP_CONF_DIR=/etc/hadoop/conf  (so core-site.xml is on the");
    LOG.warn("       classpath and fs.defaultFS points at your HDFS nameservice), OR");
    LOG.warn("    2) pass a fully-qualified path, e.g.");
    LOG.warn("         -D" + key + "=hdfs://<nameservice>/path/to/model");
    LOG.warn("================================================================");
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
