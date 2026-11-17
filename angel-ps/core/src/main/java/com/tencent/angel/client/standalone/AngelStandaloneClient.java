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

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.deploy.standalone.StandaloneRemoteLauncher;
import com.tencent.angel.master.yarn.util.AngelApps;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.StartRequest;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Angel client for {@link com.tencent.angel.AngelDeployMode#STANDALONE} deploy mode.
 *
 * <p>This client embeds the Angel ApplicationMaster in the submitting JVM (no YARN, no Spark)
 * while running each Parameter Server / Worker as an independent local JVM process.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Allocate an ApplicationId.</li>
 *   <li>Materialize the runtime configuration into a {@code job.xml} file under a private dir.</li>
 *   <li>Add that dir to the standalone classpath so child processes can pick it up via
 *       {@code conf.addResource("job.xml")}.</li>
 *   <li>Start the AM in-process; AM uses {@code StandaloneContainerLauncher} to spawn child JVMs.</li>
 * </ol>
 */
public class AngelStandaloneClient extends AngelClient {

  private static final Log LOG = LogFactory.getLog(AngelStandaloneClient.class);

  private ApplicationId appId;

  private File jobConfDir;

  /**
   * True when {@link #jobConfDir} was supplied by the user via
   * {@link AngelConf#ANGEL_STANDALONE_JOB_CONF_DIR}. We keep that case out of
   * {@link #cleanupJobConfDir()} because a shared / pre-existing directory is by
   * definition not ours to remove.
   */
  private boolean jobConfDirIsUserProvided;

  /** In-process AM, retained for test-only inspection of child process state. */
  private AngelApplicationMaster appMasterRef;

  public AngelStandaloneClient(Configuration conf) {
    super(conf);
  }

  @Override public void startPSServer() throws AngelException {
    try {
      setUser();

      Path stagingDir = AngelApps.getStagingDir(conf, userName);

      long clusterTimestamp = System.currentTimeMillis();
      int randomId = new Random().nextInt(99999);
      appId = ApplicationId.newInstance(clusterTimestamp, randomId);
      JobID jobId = TypeConverter.fromYarn(appId);

      Path submitJobDir = new Path(stagingDir, appId.toString());
      conf.set(AngelConf.ANGEL_JOB_DIR, submitJobDir.toString());
      conf.set(AngelConf.ANGEL_JOB_ID, jobId.toString());
      conf.set(AngelConf.ANGEL_EMBEDDED_APP_ID, appId.toString());

      setInputDirectory();
      setOutputDirectory();

      checkParameters(conf);
      handleDeprecatedParameters(conf);

      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 0);

      // Materialize the runtime conf into a job.xml file and add its dir to the standalone
      // classpath, so that child PS / Worker JVMs can do conf.addResource("job.xml").
      jobConfDir = prepareJobConfDir(appId);
      writeJobConf(jobConfDir);
      injectStandaloneClasspath(jobConfDir);

      // Distributed mode: ensure remote workdir exists on every target host and upload
      // job.xml so each child JVM can pick it up via classpath.
      ensureRemoteWorkdirAndJobConf(jobConfDir);

      String hostName = conf.get("angel.am.host");
      if (hostName == null || hostName.isEmpty()) {
        hostName = resolveHostName();
      }
      // Reuse fixed-but-configurable ports similar to LOCAL/EMBEDDED clients.
      int amPort = conf.getInt("angel.am.port", 9999);
      int httpPort = conf.getInt("angel.am.http.port", 8888);

      conf.setBoolean("fs.automatic.close", false);
      String appName = conf.get(AngelConf.ANGEL_JOB_NAME, "angel-standalone");

      AngelApplicationMaster appMaster =
          new AngelApplicationMaster(conf, appName, appAttemptId, null,
              hostName, amPort, httpPort,
              System.currentTimeMillis(), new Credentials());

      appMaster.initAndStart();
      this.appMasterRef = appMaster;
      masterLocation = appMaster.getMasterLocation();

      updateMaster(60);
    } catch (Exception x) {
      LOG.error("start angel master in standalone mode failed.", x);
      throw new AngelException(x);
    }
  }

  @Override public void run() throws AngelException {
    if (master == null) {
      throw new AngelException(
          "parameter servers are not started, you must execute startPSServer first!!");
    }
    try {
      master.start(null, StartRequest.newBuilder().build());
    } catch (ServiceException e) {
      LOG.error("start application failed.", e);
      throw new AngelException(e);
    }
  }

  @Override public void kill() {
    // best-effort: tear down RPC connections; child processes will time out and exit
    try {
      close();
    } catch (Throwable ignore) {
    }
  }

  @Override public void close() {
    super.close();
    cleanupJobConfDir();
  }

  @Override protected void updateMaster(int maxWaitSeconds) throws Exception {
    int tryTime = 0;
    TConnection connection = TConnectionManager.getConnection(conf);
    while (tryTime < maxWaitSeconds) {
      try {
        if (masterLocation == null) {
          Thread.sleep(1000);
          tryTime++;
          continue;
        }
        LOG.info("master host=" + masterLocation.getIp() + ", port=" + masterLocation.getPort());
        master = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
        startHeartbeat();
        return;
      } catch (Exception e) {
        LOG.error("Register to Master failed, retrying...", e);
        Thread.sleep(1000);
        tryTime++;
      }
    }
    throw new IOException("wait for master location timeout");
  }

  @Override protected String getAppId() {
    return appId.toString();
  }

  /**
   * Visible-for-testing accessor to the master rpc proxy created during {@link #updateMaster(int)}.
   */
  com.tencent.angel.master.MasterProtocol getMasterServiceForTest() {
    return master;
  }

  /**
   * Visible-for-testing accessor to the embedded {@link AngelApplicationMaster}. Tests
   * use it to inspect the child-process bookkeeping in
   * {@link com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher}.
   */
  public AngelApplicationMaster getAppMasterForTest() {
    return appMasterRef;
  }

  /**
   * Absolute path of the materialized {@code job.xml} on the local filesystem, or
   * {@code null} if it has not been written yet (e.g. {@link #startPSServer()} not
   * called). External-PS launchers need this path so each manually-started PS process
   * can pick the same configuration up via the standalone classpath.
   */
  public String getJobConfFile() {
    if (jobConfDir == null) {
      return null;
    }
    return new File(jobConfDir, AngelConf.ANGEL_JOB_CONF_FILE).getAbsolutePath();
  }

  // ---------------------------------------------------------------------------------------------

  private static String resolveHostName() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Throwable t) {
      return "127.0.0.1";
    }
  }

  private File prepareJobConfDir(ApplicationId appId) throws IOException {
    // Precedence:
    //   1. angel.standalone.job.conf.dir   (user override; typically a shared FS path)
    //   2. <angel.standalone.log.dir>/<appId>-conf  (default, master-local)
    //   3. <java.io.tmpdir>/angel-standalone-logs/<appId>-conf  (final fallback)
    String userDir = conf.get(AngelConf.ANGEL_STANDALONE_JOB_CONF_DIR);
    File dir;
    if (userDir != null && !userDir.trim().isEmpty()) {
      // Use the user-provided directory verbatim. We do NOT append "<appId>-conf" here
      // because the operator might want to point straight at a path they manage (e.g.
      // /shared/angel/clusterA/conf). They can include a per-app sub-path themselves
      // if they need to keep multiple clusters separate.
      dir = new File(userDir.trim());
      jobConfDirIsUserProvided = true;
    } else {
      String base = conf.get(AngelConf.ANGEL_STANDALONE_LOG_DIR,
          System.getProperty("java.io.tmpdir") + File.separator + "angel-standalone-logs");
      dir = new File(base, appId.toString() + "-conf");
      jobConfDirIsUserProvided = false;
    }
    if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
      throw new IOException("Failed to create standalone job conf dir: " + dir);
    }
    LOG.info("Standalone job conf dir = " + dir.getAbsolutePath()
        + (jobConfDirIsUserProvided ? " (user-provided, will NOT be deleted on shutdown)"
                                    : " (auto-managed)"));
    return dir;
  }

  private void writeJobConf(File dir) throws IOException {
    File file = new File(dir, AngelConf.ANGEL_JOB_CONF_FILE);
    try (OutputStream out = new FileOutputStream(file)) {
      conf.writeXml(out);
    }
    LOG.info("Standalone job conf written to " + file.getAbsolutePath());
  }

  private void injectStandaloneClasspath(File jobConfDir) {
    String existing = conf.get(AngelConf.ANGEL_STANDALONE_CLASSPATH,
        System.getProperty("java.class.path"));
    String merged = jobConfDir.getAbsolutePath() + File.pathSeparator + existing;
    conf.set(AngelConf.ANGEL_STANDALONE_CLASSPATH, merged);
    LOG.info("Standalone child classpath = " + merged);
  }

  private void cleanupJobConfDir() {
    if (jobConfDir == null) {
      return;
    }
    // Never delete a user-provided directory: it may be on a shared filesystem and
    // host job.xml files for other clusters. Touching it is the operator's job.
    if (jobConfDirIsUserProvided) {
      LOG.info("Leaving user-provided job conf dir intact: " + jobConfDir.getAbsolutePath());
      return;
    }
    if (conf.getBoolean(AngelConf.ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE,
        AngelConf.DEFAULT_ANGEL_JOB_REMOVE_STAGING_DIR_ENABLE)) {
      File[] files = jobConfDir.listFiles();
      if (files != null) {
        for (File f : files) {
          //noinspection ResultOfMethodCallIgnored
          f.delete();
        }
      }
      //noinspection ResultOfMethodCallIgnored
      jobConfDir.delete();
    }
  }

  /**
   * For multi-host STANDALONE deployments: pick a remote workdir, upload {@code job.xml} to
   * every target host, and persist the workdir into {@code conf} so that the launcher can
   * use it when constructing child JVM commands.
   *
   * <p>Hosts that resolve to the local machine are skipped (the local launcher reads the
   * file directly from {@link #jobConfDir} via the standalone classpath).
   */
  private void ensureRemoteWorkdirAndJobConf(File jobConfDir) throws IOException {
    Set<RemoteHost> remoteHosts = collectRemoteHosts();
    if (remoteHosts.isEmpty()) {
      return; // single-node, nothing to distribute
    }

    String workdir = conf.get(AngelConf.ANGEL_STANDALONE_REMOTE_WORKDIR);
    if (workdir == null || workdir.isEmpty()) {
      workdir = "/tmp/angel/" + appId.toString();
      conf.set(AngelConf.ANGEL_STANDALONE_REMOTE_WORKDIR, workdir);
    }

    if (conf.get(AngelConf.ANGEL_STANDALONE_REMOTE_CLASSPATH) == null) {
      throw new IOException("STANDALONE distributed mode requires "
          + AngelConf.ANGEL_STANDALONE_REMOTE_CLASSPATH
          + " to be set (path of pre-deployed Angel jars on every remote host).");
    }

    File jobXml = new File(jobConfDir, AngelConf.ANGEL_JOB_CONF_FILE);
    if (!jobXml.isFile()) {
      throw new IOException("job.xml not found at " + jobXml + ", cannot distribute");
    }

    StandaloneRemoteLauncher remote = new StandaloneRemoteLauncher(conf);
    for (RemoteHost h : remoteHosts) {
      LOG.info("Distributing job.xml to " + h.host + ":" + h.port + " under " + workdir);
      remote.remoteMkdir(h.host, h.port, workdir);
      remote.scpUpload(h.host, h.port, jobXml, workdir + "/" + AngelConf.ANGEL_JOB_CONF_FILE);
    }
  }

  private Set<RemoteHost> collectRemoteHosts() {
    int defaultPort = conf.getInt(AngelConf.ANGEL_STANDALONE_SSH_PORT,
        AngelConf.DEFAULT_ANGEL_STANDALONE_SSH_PORT);
    Set<String> localAliases = collectLocalAliases();
    Set<RemoteHost> set = new LinkedHashSet<>();
    addHosts(set, conf.get(AngelConf.ANGEL_STANDALONE_HOSTS, ""), defaultPort, localAliases);
    addHosts(set, conf.get(AngelConf.ANGEL_STANDALONE_PS_HOSTS, ""), defaultPort, localAliases);
    addHosts(set, conf.get(AngelConf.ANGEL_STANDALONE_WORKER_HOSTS, ""), defaultPort, localAliases);
    return set;
  }

  private static void addHosts(Set<RemoteHost> dst, String csv, int defaultPort,
                               Set<String> localAliases) {
    if (csv == null || csv.isEmpty()) {
      return;
    }
    for (String raw : csv.split(",")) {
      String s = raw.trim();
      if (s.isEmpty()) continue;
      // Strip an optional "*N" multiplier suffix (see StandaloneHostPool.parse). For the
      // remote distribution path we only care about the unique set of hosts, so the
      // multiplier is discarded here.
      int star = s.lastIndexOf('*');
      if (star > 0) {
        String tail = s.substring(star + 1).trim();
        try {
          int n = Integer.parseInt(tail);
          if (n > 0) {
            s = s.substring(0, star).trim();
          }
        } catch (NumberFormatException ignore) {
          // not a multiplier; leave s alone
        }
      }
      int colon = s.indexOf(':');
      String host;
      int port = defaultPort;
      if (colon >= 0) {
        host = s.substring(0, colon).trim();
        try {
          port = Integer.parseInt(s.substring(colon + 1).trim());
        } catch (NumberFormatException ignore) {
        }
      } else {
        host = s;
      }
      if (host.isEmpty()) continue;
      String h = host.toLowerCase();
      if (h.equals("localhost") || h.equals("127.0.0.1") || h.equals("::1")
          || localAliases.contains(h)) {
        continue;
      }
      dst.add(new RemoteHost(host, port));
    }
  }

  private static Set<String> collectLocalAliases() {
    Set<String> aliases = new LinkedHashSet<>();
    aliases.add("localhost");
    aliases.add("127.0.0.1");
    try {
      InetAddress addr = InetAddress.getLocalHost();
      if (addr.getHostName() != null) aliases.add(addr.getHostName().toLowerCase());
      if (addr.getHostAddress() != null) aliases.add(addr.getHostAddress().toLowerCase());
    } catch (Throwable ignore) {
    }
    return aliases;
  }

  private static final class RemoteHost {
    final String host;
    final int port;
    RemoteHost(String host, int port) {
      this.host = host;
      this.port = port;
    }
    @Override public boolean equals(Object o) {
      if (!(o instanceof RemoteHost)) return false;
      RemoteHost r = (RemoteHost) o;
      return port == r.port && host.equals(r.host);
    }
    @Override public int hashCode() {
      return host.hashCode() * 31 + port;
    }
  }
}
