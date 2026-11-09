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

import com.tencent.angel.conf.AngelConf;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Helper around the system {@code ssh} / {@code scp} client used by
 * {@link StandaloneContainerLauncher} for distributed (multi-host) STANDALONE deployments.
 *
 * <p>Passwordless SSH (key-based authentication) MUST be configured ahead of time; this
 * helper does not prompt for or pass passwords.
 */
public class StandaloneRemoteLauncher {

  private static final Log LOG = LogFactory.getLog(StandaloneRemoteLauncher.class);

  private final String sshCmd;
  private final String scpCmd;
  private final String sshUser;
  private final List<String> extraSshOpts;

  public StandaloneRemoteLauncher(Configuration conf) {
    this.sshCmd = conf.get(AngelConf.ANGEL_STANDALONE_SSH_CMD,
        AngelConf.DEFAULT_ANGEL_STANDALONE_SSH_CMD);
    this.scpCmd = conf.get(AngelConf.ANGEL_STANDALONE_SCP_CMD,
        AngelConf.DEFAULT_ANGEL_STANDALONE_SCP_CMD);
    this.sshUser = conf.get(AngelConf.ANGEL_STANDALONE_SSH_USER,
        System.getProperty("user.name"));
    this.extraSshOpts = splitOpts(conf.get(AngelConf.ANGEL_STANDALONE_SSH_OPTS, ""));
  }

  /** SSH user that will be used for the remote login. */
  public String sshUser() {
    return sshUser;
  }

  /**
   * Build the {@code ssh} command line used to launch a remote JVM.
   *
   * @param host  remote host name
   * @param port  ssh port
   * @param env   environment variables to set on the remote shell before {@code java}
   * @param remoteJavaCmd the java cmd plus arguments (already shell-safe pieces) that should
   *                      be executed on the remote host. The list is joined with spaces.
   * @return the local OS command list to pass to {@link ProcessBuilder}.
   */
  public List<String> buildSshCmd(String host, int port,
                                  Map<String, String> env,
                                  List<String> remoteJavaCmd) {
    List<String> cmd = new ArrayList<>();
    cmd.add(sshCmd);
    cmd.add("-p");
    cmd.add(String.valueOf(port));
    cmd.addAll(extraSshOpts);
    cmd.add("-o");
    cmd.add("BatchMode=yes");
    cmd.add(sshUser + "@" + host);

    StringBuilder remote = new StringBuilder();
    if (env != null && !env.isEmpty()) {
      // POSIX-style env prefix; remote shell evaluates these as VAR=value pairs.
      for (Map.Entry<String, String> e : env.entrySet()) {
        remote.append(e.getKey()).append('=').append(shellQuote(e.getValue())).append(' ');
      }
    }
    remote.append("exec ");
    boolean first = true;
    for (String tok : remoteJavaCmd) {
      if (!first) {
        remote.append(' ');
      }
      remote.append(shellQuote(tok));
      first = false;
    }
    cmd.add(remote.toString());
    return cmd;
  }

  /**
   * Run {@code ssh user@host -- mkdir -p <path>} synchronously. Idempotent.
   */
  public void remoteMkdir(String host, int port, String path) throws IOException {
    List<String> cmd = new ArrayList<>();
    cmd.add(sshCmd);
    cmd.add("-p");
    cmd.add(String.valueOf(port));
    cmd.addAll(extraSshOpts);
    cmd.add("-o");
    cmd.add("BatchMode=yes");
    cmd.add(sshUser + "@" + host);
    cmd.add("mkdir -p " + shellQuote(path));
    runAndWait(cmd, "ssh-mkdir", host);
  }

  /**
   * Upload a local file to {@code user@host:remotePath}, blocking until completion.
   */
  public void scpUpload(String host, int port, File localFile, String remotePath)
      throws IOException {
    List<String> cmd = new ArrayList<>();
    cmd.add(scpCmd);
    cmd.add("-P");
    cmd.add(String.valueOf(port));
    cmd.addAll(extraSshOpts);
    cmd.add("-o");
    cmd.add("BatchMode=yes");
    cmd.add(localFile.getAbsolutePath());
    cmd.add(sshUser + "@" + host + ":" + remotePath);
    runAndWait(cmd, "scp", host);
  }

  // ---------------------------------------------------------------------------------------------

  private void runAndWait(List<String> cmd, String tag, String host) throws IOException {
    LOG.info(tag + " -> " + host + " : " + String.join(" ", cmd));
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);
    pb.inheritIO();
    Process p = pb.start();
    int code;
    try {
      code = p.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(tag + " interrupted while talking to " + host, e);
    }
    if (code != 0) {
      throw new IOException(tag + " to " + host + " failed with exit code " + code);
    }
  }

  private static List<String> splitOpts(String s) {
    if (s == null || s.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> out = new ArrayList<>();
    for (String t : s.trim().split("\\s+")) {
      if (!t.isEmpty()) {
        out.add(t);
      }
    }
    return out;
  }

  /**
   * Single-quote a string for safe inclusion in a POSIX shell command sent over SSH.
   * Uses the standard {@code '...'} wrapping with embedded {@code '\''} for literal quotes.
   */
  public static String shellQuote(String s) {
    if (s == null) {
      return "''";
    }
    StringBuilder sb = new StringBuilder(s.length() + 2);
    sb.append('\'');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\'') {
        sb.append("'\\''");
      } else {
        sb.append(c);
      }
    }
    sb.append('\'');
    return sb.toString();
  }
}
