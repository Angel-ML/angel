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

package com.tencent.angel.master.deploy.standalone;

import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.deploy.ContainerLauncher;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.deploy.standalone.StandaloneHostPool.Target;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Container launcher for {@link com.tencent.angel.AngelDeployMode#STANDALONE} mode.
 *
 * <p>Two flavors:
 * <ul>
 *   <li><b>Single-node</b>: when no host pool is configured (see
 *       {@link AngelConf#ANGEL_STANDALONE_HOSTS}), every PS / Worker is launched as a child
 *       JVM via {@link ProcessBuilder} on the AM host.</li>
 *   <li><b>Distributed</b>: when remote hosts are configured, attempts are round-robin'd
 *       across the host pool by {@link StandaloneHostPool}; remote launches are performed
 *       by spawning {@code ssh user@host -- env=v java ...} via {@link StandaloneRemoteLauncher}.
 *       Hosts that resolve to the local machine still go through the in-process
 *       {@code ProcessBuilder} fast path.</li>
 * </ul>
 *
 * <p>For the distributed flavor the application's {@code job.xml} must already be present
 * on every remote host (uploaded by {@code AngelStandaloneClient}) and the Angel jars must
 * be deployed to {@link AngelConf#ANGEL_STANDALONE_REMOTE_CLASSPATH}.
 */
public class StandaloneContainerLauncher extends ContainerLauncher {

  private static final Log LOG = LogFactory.getLog(StandaloneContainerLauncher.class);

  private final AMContext context;

  /** Tracked child / ssh processes keyed by attempt id. */
  private final Map<Id, Process> processes = new ConcurrentHashMap<>();

  /** Watcher threads that wait on child process exit. */
  private final Map<Id, Thread> watchers = new ConcurrentHashMap<>();

  private final StandaloneHostPool hostPool;
  private final StandaloneRemoteLauncher remote;

  public StandaloneContainerLauncher(AMContext context) {
    super("standalone-container-launcher");
    this.context = context;
    Configuration conf = context == null ? new Configuration() : context.getConf();
    this.hostPool = new StandaloneHostPool(conf);
    this.remote = new StandaloneRemoteLauncher(conf);
  }

  @Override public void handle(ContainerLauncherEvent event) {
    switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        launch(event);
        break;
      case CONTAINER_REMOTE_CLEANUP:
        cleanup(event);
        break;
      default:
        break;
    }
  }

  @SuppressWarnings("unchecked") private void launch(ContainerLauncherEvent event) {
    Id id = event.getId();
    // External-PS mode: the master only registers attempts and waits for PSes to phone
    // home; it must NOT spawn any child JVM. We still fire PA_CONTAINER_LAUNCHED so the
    // PSAttempt state machine moves to RUNNING-pending-register and the attempt enters
    // the heartbeat-timeout monitor.
    if (id instanceof PSAttemptId && isExternalPs()) {
      LOG.info("External-PS mode: skipping spawn for " + id
          + "; waiting for an externally-launched PS to register.");
      context.getEventHandler()
          .handle(new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCHED, (PSAttemptId) id));
      return;
    }
    try {
      Target target = hostPool.assign(id);
      Process process;
      if (id instanceof PSAttemptId) {
        process = launchPS((PSAttemptId) id, target);
        context.getEventHandler()
            .handle(new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCHED, (PSAttemptId) id));
      } else if (id instanceof WorkerAttemptId) {
        process = launchWorker((WorkerAttemptId) id, target);
        context.getEventHandler().handle(
            new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCHED, (WorkerAttemptId) id));
      } else {
        LOG.error("Unknown attempt id type for standalone launch: " + id);
        return;
      }

      processes.put(id, process);
      startProcessWatcher(id, process);
    } catch (Exception e) {
      LOG.error("launch standalone process for " + id + " failed.", e);
      if (id instanceof PSAttemptId) {
        context.getEventHandler().handle(
            new PSAttemptEvent(PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED, (PSAttemptId) id));
      } else if (id instanceof WorkerAttemptId) {
        context.getEventHandler().handle(
            new WorkerAttemptEvent(WorkerAttemptEventType.CONTAINER_LAUNCH_FAILED,
                (WorkerAttemptId) id));
      }
    }
  }

  private boolean isExternalPs() {
    Configuration conf = context == null ? new Configuration() : context.getConf();
    return conf.getBoolean(AngelConf.ANGEL_STANDALONE_PS_EXTERNAL,
        AngelConf.DEFAULT_ANGEL_STANDALONE_PS_EXTERNAL);
  }

  private void cleanup(ContainerLauncherEvent event) {
    Id id = event.getId();
    Process process = processes.remove(id);
    Thread watcher = watchers.remove(id);
    if (watcher != null) {
      watcher.interrupt();
    }
    if (process != null && process.isAlive()) {
      LOG.info("Destroy standalone process for " + id);
      // For a remote ssh process: destroying the local ssh client closes the channel and
      // the OpenSSH server terminates the remote child, since we are using `exec` semantics
      // (no nohup). A stricter cleanup would also issue `pkill` on the remote, but is not
      // necessary for normal shutdown paths.
      process.destroy();
      try {
        process.waitFor();
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
    hostPool.release(id);
  }

  private void startProcessWatcher(final Id id, final Process process) {
    Thread watcher = new Thread(() -> {
      try {
        int exitCode = process.waitFor();
        LOG.info("Standalone process for " + id + " exited with code " + exitCode);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        processes.remove(id, process);
        watchers.remove(id);
      }
    }, "standalone-process-watcher-" + id);
    watcher.setDaemon(true);
    watcher.start();
    watchers.put(id, watcher);
  }

  // ---------------------------------------------------------------------------------------------
  // launch impls
  // ---------------------------------------------------------------------------------------------

  private Process launchPS(PSAttemptId id, Target target) throws IOException {
    Configuration conf = context.getConf();

    Map<String, String> env = baseEnv();
    env.put(AngelEnvironment.PARAMETERSERVER_ID.name(),
        String.valueOf(id.getPsId().getIndex()));
    env.put(AngelEnvironment.PS_ATTEMPT_ID.name(), String.valueOf(id.getIndex()));
    env.put(AngelEnvironment.LISTEN_ADDR.name(), masterIpForChild(conf, target));
    env.put(AngelEnvironment.LISTEN_PORT.name(),
        String.valueOf(context.getMasterService().getLocation().getPort()));
    // Tell the child JVM the routable IP it should advertise to the master. For remote
    // targets this is the ssh target host itself; for local targets we fall back to a
    // network-interface scan because InetAddress.getLocalHost() is unreliable on boxes
    // whose /etc/hosts maps the hostname to 127.0.0.1.
    String psListenHost = childListenHost(conf, target);
    if (psListenHost != null && !psListenHost.isEmpty()) {
      env.put(AngelEnvironment.PS_LISTEN_ADDR.name(), psListenHost);
    }
    env.put(AngelEnvironment.TASK_NUMBER.name(),
        String.valueOf(conf.getInt(AngelConf.ANGEL_TASK_ACTUAL_NUM,
            conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
                AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER))));

    String extraOpts = conf.get(AngelConf.ANGEL_PS_JAVA_OPTS, "");
    extraOpts = appendLogOpts(extraOpts,
        conf.get(AngelConf.ANGEL_STANDALONE_PS_LOG4J_FILE),
        conf.get(AngelConf.ANGEL_STANDALONE_PS_LOG_LEVEL),
        "angel.ps.log.level");
    return doLaunch(conf, target, "ps", id.toString(), env, extraOpts,
        "com.tencent.angel.ps.StandalonePSMain");
  }

  private Process launchWorker(WorkerAttemptId id, Target target) throws IOException {
    Configuration conf = context.getConf();

    Map<String, String> env = baseEnv();
    env.put(AngelEnvironment.WORKER_GROUP_ID.name(),
        String.valueOf(id.getWorkerId().getWorkerGroupId().getIndex()));
    env.put(AngelEnvironment.WORKER_ID.name(),
        String.valueOf(id.getWorkerId().getIndex()));
    env.put(AngelEnvironment.WORKER_ATTEMPT_ID.name(), String.valueOf(id.getIndex()));
    env.put(AngelEnvironment.LISTEN_ADDR.name(), masterIpForChild(conf, target));
    env.put(AngelEnvironment.LISTEN_PORT.name(),
        String.valueOf(context.getMasterService().getLocation().getPort()));
    String workerListenHost = childListenHost(conf, target);
    if (workerListenHost != null && !workerListenHost.isEmpty()) {
      env.put(AngelEnvironment.WORKER_LISTEN_ADDR.name(), workerListenHost);
    }
    env.put(AngelEnvironment.WORKERGROUP_NUMBER.name(),
        String.valueOf(conf.getInt(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM,
            conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
                AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER))));
    env.put(AngelEnvironment.TASK_NUMBER.name(),
        String.valueOf(conf.getInt(AngelConf.ANGEL_TASK_ACTUAL_NUM,
            conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
                AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER))));
    env.put(AngelEnvironment.INIT_MIN_CLOCK.name(), "0");
    String userTask = conf.get(AngelConf.ANGEL_TASK_USER_TASKCLASS);
    if (userTask != null) {
      env.put(AngelEnvironment.ANGEL_USER_TASK.name(), userTask);
    }

    String extraOpts = conf.get(AngelConf.ANGEL_WORKER_JAVA_OPTS, "");
    extraOpts = appendLogOpts(extraOpts,
        conf.get(AngelConf.ANGEL_STANDALONE_WORKER_LOG4J_FILE),
        conf.get(AngelConf.ANGEL_STANDALONE_WORKER_LOG_LEVEL),
        "angel.worker.log.level");
    return doLaunch(conf, target, "worker", id.toString(), env, extraOpts,
        "com.tencent.angel.worker.StandaloneWorkerMain");
  }

  /**
   * Build the {@code -Dlog4j.* / -Dangel.<role>.log.level} flags appended to the child
   * JVM's java command line. Returns the accumulated extraOpts string. Order: caller's
   * existing opts first, log opts last (so the log opts win on duplicates).
   *
   * @param baseOpts existing JVM opts (e.g. heap settings from angel.ps.java.opts)
   * @param log4jFile log4j configuration file; may be {@code null}
   * @param logLevel root-logger level convenience knob; may be {@code null}
   * @param logLevelKey system-property key under which {@code logLevel} is exposed
   *                    (e.g. {@code angel.ps.log.level}). The log4j config can pick it
   *                    up via {@code log4j.rootLogger=${angel.ps.log.level}, ...}.
   */
  private static String appendLogOpts(String baseOpts, String log4jFile,
                                      String logLevel, String logLevelKey) {
    StringBuilder sb = new StringBuilder();
    if (baseOpts != null && !baseOpts.isEmpty()) {
      sb.append(baseOpts.trim());
    }
    if (log4jFile != null && !log4jFile.trim().isEmpty()) {
      String f = log4jFile.trim();
      // Set both keys so the child works regardless of whether log4j 1.x or 2.x wins
      // on its classpath. log4j 1.x reads -Dlog4j.configuration (URL or classpath
      // resource); log4j 2.x reads -Dlog4j.configurationFile (path or URL).
      if (sb.length() > 0) sb.append(' ');
      sb.append("-Dlog4j.configuration=file:").append(f);
      sb.append(" -Dlog4j.configurationFile=").append(f);
    }
    if (logLevel != null && !logLevel.trim().isEmpty()) {
      if (sb.length() > 0) sb.append(' ');
      sb.append("-D").append(logLevelKey).append('=').append(logLevel.trim());
    }
    return sb.toString();
  }

  private Process doLaunch(Configuration conf, Target target, String role, String idStr,
                           Map<String, String> env, String extraOpts,
                           String mainClass) throws IOException {
    if (target == null || target.local) {
      return launchLocal(conf, role, idStr, env, extraOpts, mainClass);
    }
    return launchRemote(conf, target, role, idStr, env, extraOpts, mainClass);
  }

  private Process launchLocal(Configuration conf, String role, String idStr,
                              Map<String, String> env, String extraOpts,
                              String mainClass) throws IOException {
    List<String> cmd = localJavaCmd(conf, extraOpts, mainClass);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(localLogFileFor(conf, role, idStr)));
    pb.environment().putAll(env);
    pb.environment().put("USER", context.getUser());
    LOG.info("Standalone (local) launch " + role + " " + idStr + " cmd=" + String.join(" ", cmd));
    return pb.start();
  }

  private Process launchRemote(Configuration conf, Target target, String role, String idStr,
                               Map<String, String> env, String extraOpts,
                               String mainClass) throws IOException {
    String workdir = conf.get(AngelConf.ANGEL_STANDALONE_REMOTE_WORKDIR);
    if (workdir == null || workdir.isEmpty()) {
      throw new IOException("STANDALONE distributed mode requires "
          + AngelConf.ANGEL_STANDALONE_REMOTE_WORKDIR + " to be set");
    }
    String remoteCp = conf.get(AngelConf.ANGEL_STANDALONE_REMOTE_CLASSPATH);
    if (remoteCp == null || remoteCp.isEmpty()) {
      throw new IOException("STANDALONE distributed mode requires "
          + AngelConf.ANGEL_STANDALONE_REMOTE_CLASSPATH + " to be set");
    }
    String remoteLogDir = roleRemoteLogDir(conf, role);

    // Make sure log dir exists on the remote host. The workdir should already have been
    // pre-created and populated with job.xml by AngelStandaloneClient.
    remote.remoteMkdir(target.host, target.port, remoteLogDir);

    String javaBin = conf.get(AngelConf.ANGEL_STANDALONE_REMOTE_JAVA,
        AngelConf.DEFAULT_ANGEL_STANDALONE_REMOTE_JAVA);
    String classpath = workdir + ":" + remoteCp;

    List<String> javaCmd = new ArrayList<>();
    javaCmd.add(javaBin);
    if (extraOpts != null && !extraOpts.isEmpty()) {
      for (String opt : extraOpts.trim().split("\\s+")) {
        if (!opt.isEmpty()) {
          javaCmd.add(opt);
        }
      }
    }
    javaCmd.add("-cp");
    javaCmd.add(classpath);
    javaCmd.add(mainClass);

    // Redirect remote stdout/stderr so we don't tunnel everything back over the ssh channel.
    String remoteLogFile = remoteLogDir + "/" + role + "-" + sanitize(idStr) + ".log";
    List<String> wrappedCmd = new ArrayList<>();
    wrappedCmd.add("sh");
    wrappedCmd.add("-c");
    StringBuilder inner = new StringBuilder();
    boolean first = true;
    for (String t : javaCmd) {
      if (!first) inner.append(' ');
      inner.append(StandaloneRemoteLauncher.shellQuote(t));
      first = false;
    }
    inner.append(" >>").append(StandaloneRemoteLauncher.shellQuote(remoteLogFile))
         .append(" 2>&1");
    wrappedCmd.add(inner.toString());

    Map<String, String> remoteEnv = new LinkedHashMap<>();
    remoteEnv.put("USER", context.getUser());
    remoteEnv.putAll(env);

    List<String> sshCmd = remote.buildSshCmd(target.host, target.port, remoteEnv, wrappedCmd);

    ProcessBuilder pb = new ProcessBuilder(sshCmd);
    pb.redirectErrorStream(true);
    // Mirror the remote log filename locally to track which ssh channel produced what.
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(
        localLogFileFor(conf, role + "-ssh-" + target.host, idStr)));

    LOG.info("Standalone (remote " + target + ") launch " + role + " " + idStr + " cmd="
        + String.join(" ", sshCmd));
    return pb.start();
  }

  // ---------------------------------------------------------------------------------------------
  // Local helpers
  // ---------------------------------------------------------------------------------------------

  private List<String> localJavaCmd(Configuration conf, String extraOpts, String mainClass) {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator
        + (isWindows() ? "java.exe" : "java");

    String classpath = conf.get(AngelConf.ANGEL_STANDALONE_CLASSPATH,
        System.getProperty("java.class.path"));

    List<String> cmd = new ArrayList<>();
    cmd.add(javaBin);
    if (extraOpts != null && !extraOpts.isEmpty()) {
      for (String opt : extraOpts.trim().split("\\s+")) {
        if (!opt.isEmpty()) {
          cmd.add(opt);
        }
      }
    }
    cmd.add("-cp");
    cmd.add(classpath);
    cmd.add(mainClass);
    return cmd;
  }

  private File localLogFileFor(Configuration conf, String role, String idStr) throws IOException {
    String dir = roleLocalLogDir(conf, role);
    File dirFile = new File(dir);
    if (!dirFile.exists() && !dirFile.mkdirs() && !dirFile.exists()) {
      throw new IOException("Failed to create standalone log dir: " + dir);
    }
    return new File(dirFile, role + "-" + sanitize(idStr) + ".log");
  }

  /**
   * Resolve the local log directory for a given role.
   * <ol>
   *   <li>{@code angel.standalone.<role>.log.dir} (per-role override)</li>
   *   <li>{@code angel.standalone.log.dir} (global)</li>
   *   <li>{@code $java.io.tmpdir/angel-standalone-logs}</li>
   * </ol>
   * The {@code role} string may be decorated (e.g. {@code "ps-ssh-host"}); we match on
   * its prefix so ssh-wrapped redirects share the same per-role bucket.
   */
  private static String roleLocalLogDir(Configuration conf, String role) {
    String key = roleConfKey(role,
        AngelConf.ANGEL_STANDALONE_PS_LOG_DIR,
        AngelConf.ANGEL_STANDALONE_WORKER_LOG_DIR);
    String dir = (key != null) ? conf.get(key) : null;
    if (dir == null || dir.isEmpty()) {
      dir = conf.get(AngelConf.ANGEL_STANDALONE_LOG_DIR,
          System.getProperty("java.io.tmpdir") + File.separator + "angel-standalone-logs");
    }
    return dir;
  }

  /** Return the per-role conf key for a given role-decorated string, or {@code null}. */
  private static String roleConfKey(String role, String psKey, String workerKey) {
    if (role == null) return null;
    if (role.equals("ps") || role.startsWith("ps-")) return psKey;
    if (role.equals("worker") || role.startsWith("worker-")) return workerKey;
    return null;
  }

  /**
   * Resolve the remote (ssh target) log directory for a given role.
   * <ol>
   *   <li>{@code angel.standalone.<role>.log.dir} (per-role override; same key as the
   *       local case — the operator can use a single setting to mean "wherever the
   *       child runs, write logs here")</li>
   *   <li>{@code angel.standalone.remote.log.dir}</li>
   *   <li>{@link AngelConf#DEFAULT_ANGEL_STANDALONE_REMOTE_LOG_DIR}</li>
   * </ol>
   */
  private static String roleRemoteLogDir(Configuration conf, String role) {
    String key = roleConfKey(role,
        AngelConf.ANGEL_STANDALONE_PS_LOG_DIR,
        AngelConf.ANGEL_STANDALONE_WORKER_LOG_DIR);
    String dir = (key != null) ? conf.get(key) : null;
    if (dir == null || dir.isEmpty()) {
      dir = conf.get(AngelConf.ANGEL_STANDALONE_REMOTE_LOG_DIR,
          AngelConf.DEFAULT_ANGEL_STANDALONE_REMOTE_LOG_DIR);
    }
    return dir;
  }

  private static String sanitize(String s) {
    return s.replaceAll("[^A-Za-z0-9._\\-]", "_");
  }

  private Map<String, String> baseEnv() {
    return new LinkedHashMap<>();
  }

  /**
   * Determine the master ip that the spawned PS / Worker JVM should connect back to.
   *
   * <p>Order of precedence:
   * <ol>
   *   <li>Explicit {@code angel.am.host} configuration (recommended for distributed mode,
   *       set by {@code angel-cluster-up}).</li>
   *   <li>Whatever {@code MasterService} bound to. On hosts whose {@code /etc/hosts} maps
   *       the hostname to {@code 127.0.0.1}, that value can be a loopback address; in that
   *       case it is unusable from a remote PS, so we fall back to {@link #localRoutableIp()}
   *       when the target is a remote host.</li>
   * </ol>
   */
  private String masterIpForChild(Configuration conf, Target target) {
    String configured = conf.get("angel.am.host");
    if (configured != null && !configured.trim().isEmpty()) {
      return configured.trim();
    }
    String bound = context.getMasterService().getLocation().getIp();
    boolean remote = (target != null) && !target.local;
    if (remote && isLoopback(bound)) {
      String fallback = localRoutableIp();
      if (fallback != null) {
        LOG.warn("MasterService bound to loopback (" + bound
            + ") but a remote PS/Worker on " + target.host
            + " cannot reach it. Falling back to detected routable IP " + fallback
            + ". Set -Dangel.am.host=<ip> to silence this fallback.");
        return fallback;
      }
      LOG.error("MasterService is bound to loopback (" + bound
          + ") and no routable IP could be auto-detected. The remote PS/Worker on "
          + target.host + " WILL fail to connect. Set -Dangel.am.host=<ip> to fix.");
    }
    return bound;
  }

  private static boolean isLoopback(String ip) {
    if (ip == null) return true;
    return ip.equals("127.0.0.1") || ip.equals("::1") || ip.startsWith("127.");
  }

  /**
   * Resolve the IP the child PS / Worker JVM should advertise back to the master when
   * it registers. Returns {@code null} to leave the decision to the child (which will
   * fall back to {@link com.tencent.angel.utils.NetUtils#getRealLocalIP()}).
   *
   * <p>Preference order:
   * <ol>
   *   <li>For a remote target: the {@code target.host} chosen by the host pool. It is
   *       the ssh destination so it is by construction reachable by the master.</li>
   *   <li>For a local target: {@link #localRoutableIp()} — scans network interfaces
   *       instead of relying on {@code InetAddress.getLocalHost()} which returns a
   *       loopback address on hosts with an {@code /etc/hosts} entry like
   *       {@code 127.0.0.1 &lt;hostname&gt;}.</li>
   * </ol>
   */
  private String childListenHost(Configuration conf, Target target) {
    if (target != null && !target.local && target.host != null
        && !target.host.trim().isEmpty()) {
      return target.host.trim();
    }
    return localRoutableIp();
  }

  /**
   * Best-effort: pick the first non-loopback, non-link-local IPv4 address of any UP
   * interface. Returns null if no usable address can be determined.
   */
  private static String localRoutableIp() {
    try {
      java.util.Enumeration<java.net.NetworkInterface> nics =
          java.net.NetworkInterface.getNetworkInterfaces();
      while (nics != null && nics.hasMoreElements()) {
        java.net.NetworkInterface nic = nics.nextElement();
        if (!nic.isUp() || nic.isLoopback() || nic.isVirtual()) continue;
        java.util.Enumeration<java.net.InetAddress> addrs = nic.getInetAddresses();
        while (addrs.hasMoreElements()) {
          java.net.InetAddress a = addrs.nextElement();
          if (a.isLoopbackAddress() || a.isLinkLocalAddress()
              || a.isAnyLocalAddress() || a.isMulticastAddress()) continue;
          if (a instanceof java.net.Inet4Address) {
            return a.getHostAddress();
          }
        }
      }
    } catch (Throwable t) {
      LOG.warn("localRoutableIp() failed: " + t.getMessage());
    }
    return null;
  }

  private static boolean isWindows() {
    String os = System.getProperty("os.name");
    return os != null && os.toLowerCase().startsWith("windows");
  }

  @Override protected void serviceStop() throws Exception {
    for (Map.Entry<Id, Process> entry : processes.entrySet()) {
      try {
        if (entry.getValue().isAlive()) {
          entry.getValue().destroy();
        }
      } catch (Throwable t) {
        LOG.warn("destroy process for " + entry.getKey() + " failed", t);
      }
    }
    processes.clear();
    for (Thread watcher : watchers.values()) {
      watcher.interrupt();
    }
    watchers.clear();
    super.serviceStop();
  }

  /**
   * Test hook: number of <i>alive</i> child JVM / ssh processes currently being tracked
   * by this launcher. Used by integration tests to observe PS-side liveness without
   * having to scrape OS-level process tables.
   */
  public int aliveChildCount() {
    int n = 0;
    for (Process p : processes.values()) {
      if (p != null && p.isAlive()) {
        n++;
      }
    }
    return n;
  }

  /** Visible-for-testing accessor to the configured host pool. */
  StandaloneHostPool getHostPoolForTest() {
    return hostPool;
  }
}
