/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Add or modify some parameters for Angel; Add shutDown method to fix Angel client exit problem.
 */
package com.tencent.angel.ipc;

import com.tencent.angel.Chore;
import com.tencent.angel.Stoppable;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.RemoteException;
import com.tencent.angel.io.Addressing;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ps.impl.PSProtocol;
import com.tencent.angel.worker.WorkerProtocol;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class TConnectionManager {

  private static final Logger LOG = LoggerFactory.getLogger(TConnectionManager.class);

  static final Map<TConnectionKey, TConnectionImplementation> ML_INSTANCES =
      new LinkedHashMap<TConnectionKey, TConnectionImplementation>();

  /**
   * Get the connection that goes with the passed <code>conf</code> configuration instance. If no
   * current connection exists, method creates a new connection for the passed <code>conf</code>
   * instance.
   * 
   */
  public static TConnection getConnection(Configuration conf) {
    TConnectionKey connectionKey = new TConnectionKey(conf);
    synchronized (ML_INSTANCES) {
      TConnectionImplementation connection = ML_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = new TConnectionImplementation(conf, true);
        ML_INSTANCES.put(connectionKey, connection);
      }
      connection.incCount();
      return connection;
    }
  }

  public static class TConnectionImplementation implements TConnection, Closeable {

    static final Logger LOG = LoggerFactory.getLogger(TConnectionImplementation.class);

    private final Class<? extends PSProtocol> psClientClass;

    public static final String PS_CLIENT_PROTOCOL_CLASS = "ml.ps.client.protocol.class";

    /** Default client protocol class name. */
    public static final String DEFAULT_PS_CLIENT_PROTOCOL_CLASS = PSProtocol.class.getName();

    private final Class<? extends PSProtocol.AsyncProtocol> asyncPSClientClass;

    public static final String ASYNC_PS_CLIENT_PROTOCOL_CLASS = "ml.ps.async.client.protocol.class";

    /** Default ps client protocol class name. */
    public static final String DEFAULT_PS_ASYNC_CLIENT_PROTOCOL_CLASS =
        PSProtocol.AsyncProtocol.class.getName();

        private final Class<? extends WorkerProtocol> workerClientClass;

    /** Parameter name for what client protocol to use. */
    public static final String WORKER_CLIENT_PROTOCOL_CLASS = "ml.worker.client.protocol.class";

    /** Default client protocol class name. */
    public static final String DEFAULT_WORKER_CLIENT_PROTOCOL_CLASS = WorkerProtocol.class
    .getName();

    private final Class<? extends MasterProtocol> masterClientClass;

    /** Parameter name for what client protocol to use. */
    public static final String MASTER_CLIENT_PROTOCOL_CLASS = "ml.master.client.protocol.class";

    /** Default client protocol class name. */
    public static final String DEFAULT_MASTER_CLIENT_PROTOCOL_CLASS = MasterProtocol.class
        .getName();

    private final int rpcTimeout;
    private final int maxRPCAttempts;

    private final Configuration conf;

    private volatile boolean closed;

    // indicates whether this connection's life cycle is managed
    private final boolean managed;

    private final ConcurrentHashMap<String, Map<String, VersionedProtocol>> servers =
        new ConcurrentHashMap<String, Map<String, VersionedProtocol>>();

    private final ConcurrentHashMap<String, String> connectionLock =
        new ConcurrentHashMap<String, String>();

    private boolean stopProxy = true;

    private int refCount;

    private final DelayedClosing delayedClosing = DelayedClosing.createAndStart(this);

    /**
     * constructor
     * 
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public TConnectionImplementation(Configuration conf, boolean managed) {
      this.conf = conf;
      this.managed = managed;
      this.closed = false;
      try {
        String clientClassName =
            conf.get(PS_CLIENT_PROTOCOL_CLASS, DEFAULT_PS_CLIENT_PROTOCOL_CLASS);
        this.psClientClass = (Class<? extends PSProtocol>) Class.forName(clientClassName);

        String asyncClassName =
            conf.get(ASYNC_PS_CLIENT_PROTOCOL_CLASS, DEFAULT_PS_ASYNC_CLIENT_PROTOCOL_CLASS);
        this.asyncPSClientClass =
            (Class<? extends PSProtocol.AsyncProtocol>) Class.forName(asyncClassName);

        clientClassName =
            conf.get(WORKER_CLIENT_PROTOCOL_CLASS, DEFAULT_WORKER_CLIENT_PROTOCOL_CLASS);
        this.workerClientClass = (Class<? extends WorkerProtocol>) Class.forName(clientClassName);
        clientClassName =
            conf.get(MASTER_CLIENT_PROTOCOL_CLASS, DEFAULT_MASTER_CLIENT_PROTOCOL_CLASS);
        this.masterClientClass = (Class<? extends MasterProtocol>) Class.forName(clientClassName);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(e);
      }

      this.rpcTimeout =
          conf.getInt(AngelConf.ML_RPC_TIMEOUT_KEY, AngelConf.DEFAULT_ML_RPC_TIMEOUT);
      this.maxRPCAttempts =
          conf.getInt(AngelConf.ML_CLIENT_RPC_MAXATTEMPTS,
              AngelConf.DEFAULT_ML_CLIENT_RPC_MAXATTEMPTS);
    }

    @Override
    public Configuration getConfiguration() {
      return this.conf;
    }

    /**
     * Return if this client has no reference
     * 
     * @return true if this client has no reference; false otherwise
     */
    boolean isZeroReference() {
      return refCount == 0;
    }

    /**
     * Increment this client's reference count.
     */
    void incCount() {
      ++refCount;
    }

    /**
     * Decrement this client's reference count.
     */
    void decCount() {
      if (refCount > 0) {
        --refCount;
      }
    }

    /**
     * Either the passed <code>isa</code> is null or <code>hostname</code> can be but not both.
     * 
     * @param hostname
     * @param port
     * @param protocolClass
     * @param version
     * @return Proxy.
     * @throws java.io.IOException
     */
    VersionedProtocol getProtocol(final String hostname, final int port,
        final Class<? extends VersionedProtocol> protocolClass, final long version,
        List<String> addrList4Failover) throws IOException {
      String rsName = Addressing.createHostAndPortStr(hostname, port);
      // See if we already have a connection (common case)
      Map<String, VersionedProtocol> protocols = this.servers.get(rsName);
      if (protocols == null) {
        protocols = new HashMap<String, VersionedProtocol>();
        Map<String, VersionedProtocol> existingProtocols =
            this.servers.putIfAbsent(rsName, protocols);
        if (existingProtocols != null) {
          protocols = existingProtocols;
        }
      }
      String protocol = protocolClass.getName();
      VersionedProtocol server = protocols.get(protocol);
      if (server == null) {
        // create a unique lock for this RS + protocol (if necessary)
        String lockKey = protocol + "@" + rsName;
        this.connectionLock.putIfAbsent(lockKey, lockKey);
        // get the RS lock
        synchronized (this.connectionLock.get(lockKey)) {
          // do one more lookup in case we were stalled above
          server = protocols.get(protocol);
          if (server == null) {
            try {
              // Only create isa when we need to.
              InetSocketAddress address = new InetSocketAddress(hostname, port);
              // definitely a cache miss. establish an RPC for
              // this RS
              server =
                  MLRPC.waitForProxy(protocolClass, version, address, this.conf,
                      this.maxRPCAttempts, this.rpcTimeout, this.rpcTimeout, addrList4Failover);
              protocols.put(protocol, server);
            } catch (RemoteException e) {
              LOG.warn("RemoteException connecting to RS", e);
              // Throw what the RemoteException was carrying.
              throw e.unwrapRemoteException();
            }
          }
        }
      }
      return server;
    }

    @Override
    public boolean isClosed() {
      return this.closed;
    }

    /**
     * @return the refCount
     */
    public int getRefCount() {
      return refCount;
    }

    public void stopProxyOnClose(boolean stopProxy) {
      this.stopProxy = stopProxy;
    }

    void close(boolean stopProxy) {
      if (this.closed) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.error("closing connection, stopProxy: " + stopProxy);
      }
      delayedClosing.stop("Closing connection");
      if (stopProxy) {
        for (Map<String, VersionedProtocol> i : servers.values()) {
          for (VersionedProtocol server : i.values()) {
            LOG.debug("to stop MLRPC proxy!");
            MLRPC.stopProxy(server);
          }
        }
      }
      this.servers.clear();
      this.closed = true;
    }

    @Override
    public void close() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("connection is closing!");
      }
      if (managed) {
        TConnectionManager.deleteConnection(this, stopProxy, false);
      } else {
        close(true);
      }
    }

    /**
     * Creates a Chore thread to check the connections to master & zookeeper and close them when
     * they reach their closing time ( {@link MasterProtocolState} and
     * {@link #keepZooKeeperWatcherAliveUntil} ). Keep alive time is managed by the release
     * functions and the variable {@link #keepAlive}
     */
    private static class DelayedClosing extends Chore implements Stoppable {
      private TConnectionImplementation hci;
      Stoppable stoppable;

      private DelayedClosing(TConnectionImplementation hci, Stoppable stoppable) {
        super("ZooKeeperWatcher and Master delayed closing for connection " + hci, 60 * 1000, // We
                                                                                              // check
                                                                                              // every
                                                                                              // minutes
            stoppable);
        this.hci = hci;
        this.stoppable = stoppable;
      }

      static DelayedClosing createAndStart(TConnectionImplementation hci) {
        Stoppable stoppable = new Stoppable() {
          private volatile boolean isStopped = false;

          @Override
          public void stop(String why) {
            isStopped = true;
          }

          @Override
          public boolean isStopped() {
            return isStopped;
          }
        };

        return new DelayedClosing(hci, stoppable);
      }

      @Override
      protected void chore() {
        // TODO
      }

      @Override
      public void stop(String why) {
        stoppable.stop(why);
      }

      @Override
      public boolean isStopped() {
        return stoppable.isStopped();
      }
    }

    @Override
    public PSProtocol getPSService(String hostname, int port) throws IOException {
      return (PSProtocol) getProtocol(hostname, port, psClientClass, 0L, null);
    }

    @Override
    public PSProtocol.AsyncProtocol getAsyncPSService(String hostname, int port) throws IOException {
      return (PSProtocol.AsyncProtocol) getProtocol(hostname, port, asyncPSClientClass, 0L, null);
    }

        @Override
        public WorkerProtocol getWorkerService(String hostname, int port)
                throws IOException {
            LOG.info("workerClientClass="+workerClientClass.getName());
            return (WorkerProtocol) getProtocol(hostname, port, workerClientClass,
                    0L, null);
        }

    @Override
    public MasterProtocol getMasterService(String hostname, int port) throws IOException {
      return (MasterProtocol) getProtocol(hostname, port, masterClientClass, 0L, null);
    }
  }

  static class TConnectionKey {
    public static String[] CONNECTION_PROPERTIES = new String[] {};

    private Map<String, String> properties;
    private String username;

    public TConnectionKey(Configuration conf) {
      Map<String, String> m = new HashMap<String, String>();
      if (conf != null) {
        for (String property : CONNECTION_PROPERTIES) {
          String value = conf.get(property);
          if (value != null) {
            m.put(property, value);
          }
        }
      }
      this.properties = Collections.unmodifiableMap(m);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (username != null) {
        result = username.hashCode();
      }
      for (String property : CONNECTION_PROPERTIES) {
        String value = properties.get(property);
        if (value != null) {
          result = prime * result + value.hashCode();
        }
      }

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TConnectionKey that = (TConnectionKey) obj;
      if (this.username != null && !this.username.equals(that.username)) {
        return false;
      } else if (this.username == null && that.username != null) {
        return false;
      }
      if (this.properties == null) {
        if (that.properties != null) {
          return false;
        }
      } else {
        if (that.properties == null) {
          return false;
        }
        for (String property : CONNECTION_PROPERTIES) {
          String thisValue = this.properties.get(property);
          String thatValue = that.properties.get(property);
          if (thisValue == thatValue) {
            continue;
          }
          if (thisValue == null || !thisValue.equals(thatValue)) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return "TConnectionKey{" + "properties=" + properties + ", username='" + username + '\''
          + '}';
    }
  }

  private static void deleteConnection(TConnection connection, boolean stopProxy,
      boolean staleConnection) {
    synchronized (ML_INSTANCES) {
      for (Entry<TConnectionKey, TConnectionImplementation> connectionEntry : ML_INSTANCES
          .entrySet()) {
        if (connectionEntry.getValue() == connection) {
          deleteConnection(connectionEntry.getKey(), stopProxy, staleConnection);
          break;
        }
      }
    }
  }

  private static void deleteConnection(TConnectionKey connectionKey, boolean stopProxy,
      boolean staleConnection) {
    synchronized (ML_INSTANCES) {
      TConnectionImplementation connection = ML_INSTANCES.get(connectionKey);
      if (connection != null) {
        connection.decCount();
        if (connection.isZeroReference() || staleConnection) {
          LOG.debug("to remove connectionKey: ", connectionKey);
          ML_INSTANCES.remove(connectionKey);
          connection.close(stopProxy);
        } else if (stopProxy) {
          connection.stopProxyOnClose(stopProxy);
        }
      } else {
        LOG.error("Connection not found in the list, can't delete it " + "(connection key="
            + connectionKey + "). May be the key was modified?");
      }
    }
  }

  /**
   * Delete connection information for the instance specified by configuration. If there are no more
   * references to it, this will then close connection to the zookeeper ensemble and let go of all
   * resources.
   * 
   * @param conf configuration whose identity is used to find {@link TConnection} instance.
   * @param stopProxy Shuts down all the proxy's put up to cluster members including to cluster
   *        TMaster. .
   */
  public static void deleteConnection(Configuration conf, boolean stopProxy) {
    deleteConnection(new TConnectionKey(conf), stopProxy, false);
  }

  /**
   * Delete information for all connections.
   * 
   * @param stopProxy stop the proxy as well
   * @throws java.io.IOException
   */
  public static void deleteAllConnections(boolean stopProxy) {
    synchronized (ML_INSTANCES) {
      Set<TConnectionKey> connectionKeys = new HashSet<TConnectionKey>();
      connectionKeys.addAll(ML_INSTANCES.keySet());
      for (TConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey, stopProxy, false);
      }
      ML_INSTANCES.clear();
    }
  }

  public static void shutDown(){
    MLRPC.shutDown();
  }
}
