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

package com.tencent.angel.client.attach;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Angel client that <b>attaches</b> to an <em>externally-launched</em> Angel master+PS cluster
 * (typically brought up by {@link com.tencent.angel.utils.AngelPSClusterMain} / {@code
 * bin/angel-cluster-up}) instead of launching a new one.
 *
 * <p>This client is lifecycle-read-mostly from the perspective of the attached JVM:
 * <ul>
 *   <li>{@link #startPSServer()} does NOT spawn an AM / PS; it only establishes an RPC
 *       connection to the existing master and registers this JVM as a heartbeat client.</li>
 *   <li>Model control APIs such as {@link #save}, {@link #load}, {@link #checkpoint},
 *       {@link #recover}, {@link #createMatrices}, {@link #waitForAllPS} are fully
 *       usable and are forwarded to the same master RPCs as other clients.</li>
 *   <li>{@link #stop(int)} / {@link #kill()} are <b>no-ops against the remote cluster</b>:
 *       only the local RPC / heartbeat state is torn down. The cluster lifecycle must be
 *       driven by whoever launched it.</li>
 * </ul>
 *
 * <p>Construction: either pass the master {@link Location} explicitly, or set
 * {@code angel.master.host}/{@code angel.master.port} (or {@code angel.am.host}/
 * {@code angel.am.port}) in {@link Configuration} and use {@link #AngelAttachClient(Configuration)}.
 * An application id, used only for client-side logging and for
 * {@code getJobReport(appId)} RPCs, is picked up from
 * {@link AngelConf#ANGEL_APPLICATION_ID} or {@link AngelConf#ANGEL_EMBEDDED_APP_ID}, or
 * defaults to {@code "external-angel-ps"}.
 */
public class AngelAttachClient extends AngelClient {

  private static final Log LOG = LogFactory.getLog(AngelAttachClient.class);

  /** Configuration key for the externally-managed master host (driver-side). */
  public static final String ANGEL_MASTER_HOST = "angel.master.host";
  /** Configuration key for the externally-managed master port (driver-side). */
  public static final String ANGEL_MASTER_PORT = "angel.master.port";

  private static final String DEFAULT_APP_ID = "external-angel-ps";

  private final String appId;

  /**
   * Create an attach client that resolves the master location from {@code conf}.
   * <p>Accepted keys (checked in order):
   * <ol>
   *   <li>{@code angel.master.host} + {@code angel.master.port}</li>
   *   <li>{@code angel.am.host} + {@code angel.am.port}</li>
   * </ol>
   */
  public AngelAttachClient(Configuration conf) {
    this(conf, resolveMaster(conf));
  }

  /**
   * Create an attach client bound to an explicit master {@link Location}. The master
   * location is also persisted into {@code conf} so that downstream components
   * (e.g. {@code PSAgent}) can rediscover it from configuration alone.
   */
  public AngelAttachClient(Configuration conf, Location master) {
    super(conf);
    if (master == null) {
      throw new IllegalArgumentException(
          "Master location is null. Set " + ANGEL_MASTER_HOST + "/" + ANGEL_MASTER_PORT
              + " in the configuration or pass a non-null Location.");
    }
    this.masterLocation = master;
    // Make the external master address discoverable via conf for anything that builds a
    // PSAgent or a secondary client off the same configuration.
    conf.set(ANGEL_MASTER_HOST, master.getIp());
    conf.setInt(ANGEL_MASTER_PORT, master.getPort());

    String id = conf.get(AngelConf.ANGEL_APPLICATION_ID);
    if (id == null || id.isEmpty()) {
      id = conf.get(AngelConf.ANGEL_EMBEDDED_APP_ID);
    }
    this.appId = (id == null || id.isEmpty()) ? DEFAULT_APP_ID : id;
  }

  private static Location resolveMaster(Configuration conf) {
    String host = conf.get(ANGEL_MASTER_HOST);
    int port = conf.getInt(ANGEL_MASTER_PORT, -1);
    if (host == null || host.isEmpty() || port <= 0) {
      host = conf.get("angel.am.host");
      port = conf.getInt("angel.am.port", -1);
    }
    if (host == null || host.isEmpty() || port <= 0) {
      return null;
    }
    return new Location(host, port);
  }

  // ---------------------------------------------------------------------------------------------
  // Lifecycle: connect-only; never spawn an AM/PS.
  // ---------------------------------------------------------------------------------------------

  /**
   * Establishes the RPC connection to the externally-launched master and starts the
   * keep-alive heartbeat thread. Does not create any AM / PS processes.
   */
  @Override public void startPSServer() throws AngelException {
    try {
      setUser();
      updateMaster(conf.getInt(
          AngelConf.ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS) / 1000);
      LOG.info("Attached to Angel master at " + masterLocation.getIp()
          + ":" + masterLocation.getPort() + ", appId=" + appId);
    } catch (Exception x) {
      throw new AngelException("failed to attach to external Angel master", x);
    }
  }

  @Override protected void updateMaster(int maxWaitSeconds) throws Exception {
    TConnection connection = TConnectionManager.getConnection(conf);
    int tried = 0;
    Exception last = null;
    // At least one attempt even when maxWaitSeconds <= 0.
    int budget = Math.max(1, maxWaitSeconds);
    while (tried < budget) {
      try {
        master = connection.getMasterService(masterLocation.getIp(), masterLocation.getPort());
        startHeartbeat();
        return;
      } catch (Exception e) {
        last = e;
        LOG.warn("connecting to external master " + masterLocation.getIp() + ":"
            + masterLocation.getPort() + " failed (attempt " + (tried + 1) + "/" + budget + ")", e);
        Thread.sleep(1000);
        tried++;
      }
    }
    throw new IOException("failed to connect to external Angel master at "
        + masterLocation.getIp() + ":" + masterLocation.getPort(), last);
  }

  /**
   * Override {@link AngelClient#stop(int)} to <b>NOT</b> send a StopRequest to the master.
   * We are attached to a cluster we do not own; only tear down client-side RPC / heartbeat
   * state. The caller that launched the cluster remains responsible for stopping it.
   *
   * <p>We delegate to {@link AngelClient#stop(int)} after nulling the {@code master} field
   * so the base class takes the "kill"-branch (which is a no-op here), skipping the
   * destructive {@code master.stop} RPC. This keeps the heartbeat shutdown / connection
   * cleanup logic in a single place.
   */
  @Override public void stop(int stateCode) throws AngelException {
    LOG.info("detaching from external Angel master (local tear-down only), stateCode=" + stateCode);
    master = null; // skip StopRequest to the externally-managed master
    super.stop(stateCode);
  }

  @Override public void stop() throws AngelException {
    stop(0);
  }

  /** No remote cluster is owned by this client, nothing to kill. */
  @Override public void kill() throws AngelException {
    // no-op
  }

  @Override public void close() {
    // super.close() shuts down RPC connections; that's all we need.
    super.close();
  }

  @Override protected String getAppId() {
    return appId;
  }

  /**
   * Public accessor for the application id; the base class exposes {@code getAppId()}
   * only as {@code protected}. Callers outside the {@code com.tencent.angel.client}
   * package hierarchy (e.g. Spark-on-Angel) go through this method.
   */
  public String getAppIdString() {
    return appId;
  }
}
