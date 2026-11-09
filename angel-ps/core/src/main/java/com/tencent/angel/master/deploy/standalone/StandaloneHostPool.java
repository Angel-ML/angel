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

import com.tencent.angel.common.Id;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.worker.WorkerAttemptId;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Resolves the per-attempt target {@link Target host} for STANDALONE distributed deployment.
 *
 * <p>Configuration:
 * <ul>
 *   <li>{@link AngelConf#ANGEL_STANDALONE_HOSTS} — global host pool</li>
 *   <li>{@link AngelConf#ANGEL_STANDALONE_PS_HOSTS} — PS-only override</li>
 *   <li>{@link AngelConf#ANGEL_STANDALONE_WORKER_HOSTS} — Worker-only override</li>
 * </ul>
 *
 * <p>If the resolved pool is empty or contains only the loopback / local hostname, the
 * launcher should fall back to in-process {@code ProcessBuilder} (no SSH).
 *
 * <p>Allocation strategy is simple round-robin per role, deterministic given the same
 * arrival order. Once an attempt is assigned to a host, the assignment is cached so that
 * subsequent cleanup events resolve to the same host.
 */
public class StandaloneHostPool {

  private static final Log LOG = LogFactory.getLog(StandaloneHostPool.class);

  /** A single resolved target. */
  public static class Target {
    public final String host;
    /** SSH port if remote; ignored when {@link #local} is true. */
    public final int port;
    public final boolean local;

    Target(String host, int port, boolean local) {
      this.host = host;
      this.port = port;
      this.local = local;
    }

    @Override public String toString() {
      return (local ? "local:" : "ssh:") + host + ":" + port;
    }
  }

  private final List<Target> psHosts;
  private final List<Target> workerHosts;
  private final AtomicInteger psCursor = new AtomicInteger();
  private final AtomicInteger workerCursor = new AtomicInteger();
  private final Map<Id, Target> assignments = new ConcurrentHashMap<>();

  public StandaloneHostPool(Configuration conf) {
    int defaultPort = conf.getInt(AngelConf.ANGEL_STANDALONE_SSH_PORT,
        AngelConf.DEFAULT_ANGEL_STANDALONE_SSH_PORT);

    Set<String> localAliases = collectLocalAliases();

    String global = conf.get(AngelConf.ANGEL_STANDALONE_HOSTS, "");
    String psOverride = conf.get(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "");
    String workerOverride = conf.get(AngelConf.ANGEL_STANDALONE_WORKER_HOSTS, "");

    this.psHosts = parse(
        psOverride.isEmpty() ? global : psOverride, defaultPort, localAliases);
    this.workerHosts = parse(
        workerOverride.isEmpty() ? global : workerOverride, defaultPort, localAliases);

    LOG.info("StandaloneHostPool initialized: ps=" + psHosts + ", worker=" + workerHosts);
  }

  /** True when neither PS nor Worker has any explicit host configured (single-node mode). */
  public boolean isSingleNode() {
    return psHosts.isEmpty() && workerHosts.isEmpty();
  }

  /** Pick (or look up an already-assigned) target for the given attempt id. */
  public Target assign(Id id) {
    Target existing = assignments.get(id);
    if (existing != null) {
      return existing;
    }
    Target picked = pickFor(id);
    Target prev = assignments.putIfAbsent(id, picked);
    return prev != null ? prev : picked;
  }

  /** Look up an existing assignment, returning {@code null} if there is none. */
  public Target peek(Id id) {
    return assignments.get(id);
  }

  /** Drop the cached assignment, e.g. after the attempt is fully cleaned up. */
  public void release(Id id) {
    assignments.remove(id);
  }

  // ---------------------------------------------------------------------------------------------

  private Target pickFor(Id id) {
    List<Target> pool = (id instanceof PSAttemptId) ? psHosts
        : (id instanceof WorkerAttemptId) ? workerHosts : Collections.<Target>emptyList();
    if (pool.isEmpty()) {
      return new Target(null, 0, true);
    }
    AtomicInteger cursor = (id instanceof PSAttemptId) ? psCursor : workerCursor;
    int idx = Math.floorMod(cursor.getAndIncrement(), pool.size());
    return pool.get(idx);
  }

  private static List<Target> parse(String csv, int defaultPort, Set<String> localAliases) {
    List<Target> out = new ArrayList<>();
    if (csv == null || csv.isEmpty()) {
      return out;
    }
    for (String raw : csv.split(",")) {
      String s = raw.trim();
      if (s.isEmpty()) {
        continue;
      }
      // Multiplier suffix: "ip*N" or "ip:port*N" repeats the target N times, giving the
      // round-robin allocator N distinct slots on that host. Use it to stack multiple
      // PS (or Worker) JVMs on the same machine without having to list the same IP N
      // times. "*0" explicitly drops the entry; a negative or malformed number is
      // ignored (the '*' is treated as part of the host name, matching the legacy
      // behaviour for accidental literal stars).
      int repeat = 1;
      int star = s.lastIndexOf('*');
      if (star > 0) {
        String tail = s.substring(star + 1).trim();
        try {
          int n = Integer.parseInt(tail);
          if (n > 0) {
            repeat = n;
            s = s.substring(0, star).trim();
          } else if (n == 0) {
            LOG.info("Dropping host entry '" + raw + "' because its multiplier is 0");
            continue;
          } else {
            LOG.warn("Ignoring negative multiplier in host entry '" + raw + "'");
          }
        } catch (NumberFormatException nfe) {
          // Not actually a multiplier; fall through and treat the '*' as part of the host.
        }
      }

      String host;
      int port = defaultPort;
      int colon = s.indexOf(':');
      if (colon >= 0) {
        host = s.substring(0, colon).trim();
        try {
          port = Integer.parseInt(s.substring(colon + 1).trim());
        } catch (NumberFormatException nfe) {
          LOG.warn("Invalid port in host entry '" + s + "', falling back to default " + defaultPort);
        }
      } else {
        host = s;
      }
      if (host.isEmpty()) {
        continue;
      }
      boolean local = isLocal(host, localAliases);
      // Reuse the same Target instance for all slots: downstream only reads host/port/local
      // and never mutates. The round-robin allocator keys off list index, not object identity,
      // so sharing an instance is safe and saves allocations on large clusters.
      Target t = new Target(host, port, local);
      for (int i = 0; i < repeat; i++) {
        out.add(t);
      }
    }
    return out;
  }

  private static boolean isLocal(String host, Set<String> localAliases) {
    if (host == null) {
      return true;
    }
    String h = host.toLowerCase();
    if (h.equals("localhost") || h.equals("127.0.0.1") || h.equals("::1")) {
      return true;
    }
    return localAliases.contains(h);
  }

  private static Set<String> collectLocalAliases() {
    Set<String> aliases = new LinkedHashSet<>();
    aliases.add("localhost");
    aliases.add("127.0.0.1");
    try {
      InetAddress addr = InetAddress.getLocalHost();
      if (addr.getHostName() != null) {
        aliases.add(addr.getHostName().toLowerCase());
      }
      if (addr.getHostAddress() != null) {
        aliases.add(addr.getHostAddress().toLowerCase());
      }
    } catch (Throwable ignore) {
    }
    return aliases;
  }
}
