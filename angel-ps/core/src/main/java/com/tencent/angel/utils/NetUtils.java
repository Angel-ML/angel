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

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.InvalidParameterException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Socket utils
 */
public class NetUtils {
  private static final Log LOG = LogFactory.getLog(NetUtils.class);

  public static SocketFactory getDefaultSocketFactory(Configuration conf) {
    return SocketFactory.getDefault();
  }

  public static String normalizeHostName(String name) {
    if (Character.digit(name.charAt(0), 10) != -1)
      return name;
    try {
      InetAddress ipAddress = InetAddress.getByName(name);
      return ipAddress.getHostAddress();
    } catch (UnknownHostException e) {
    }
    return name;
  }

  public static List<String> normalizeHostNames(Collection<String> names) {
    List hostNames = new ArrayList(names.size());
    for (String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }

  public static void verifyHostnames(String[] names) throws UnknownHostException {
    for (String name : names) {
      if (name == null) {
        throw new UnknownHostException("null hostname found");
      }

      URI uri = null;
      try {
        uri = new URI(name);
        if (uri.getHost() == null)
          uri = new URI("http://" + name);
      } catch (URISyntaxException e) {
        uri = null;
      }
      if ((uri == null) || (uri.getHost() == null))
        throw new UnknownHostException(name + " is not a valid Inet address");
    }
  }

  public static InetAddress getLocalInetAddress(String host) throws SocketException {
    if (host == null) {
      return null;
    }
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(host);
      if (NetworkInterface.getByInetAddress(addr) == null)
        addr = null;
    } catch (UnknownHostException ignore) {
    }
    return addr;
  }

  public static InetSocketAddress getRealLocalAddr(InetSocketAddress listenAddr)
    throws UnknownHostException {
    InetSocketAddress ret = null;
    if (listenAddr.isUnresolved() || listenAddr.getAddress().isAnyLocalAddress() || listenAddr
      .getAddress().isLoopbackAddress()) {
      ret = new InetSocketAddress(InetAddress.getLocalHost(), listenAddr.getPort());
    } else {
      ret = listenAddr;
    }

    return ret;
  }

  /**
   * Best-effort resolution of a routable local IPv4 address.
   *
   * <p>We first try {@link InetAddress#getLocalHost()}; many hosts, however, have
   * {@code /etc/hosts} entries that resolve the hostname to {@code 127.0.0.1}, in which
   * case that method returns a loopback address that is useless for cluster-facing
   * advertisement (e.g. PS &rarr; Master registration, Master RPC bind). When the primary
   * lookup yields a loopback / wildcard / link-local / site-local address we fall back to
   * scanning all {@code UP}, non-loopback, non-virtual interfaces and return the first
   * non-loopback, non-link-local, non-wildcard IPv4 address we find. Returns the
   * loopback address as a last resort (preserving the pre-existing behaviour) so the
   * throws-spec does not change.
   */
  public static String getRealLocalIP() throws UnknownHostException {
    InetAddress primary = null;
    try {
      primary = InetAddress.getLocalHost();
      if (!isUnusableForAdvertise(primary) && primary instanceof Inet4Address) {
        return primary.getHostAddress();
      }
    } catch (UnknownHostException ignore) {
      // Fall through to the interface-scan path.
    }

    String scanned = scanRoutableIpv4();
    if (scanned != null) {
      return scanned;
    }

    // Nothing better found: return whatever getLocalHost() gave us, or re-throw.
    if (primary != null) {
      return primary.getHostAddress();
    }
    return InetAddress.getLocalHost().getHostAddress();
  }

  /**
   * Enumerate network interfaces and return the first non-loopback, non-link-local,
   * non-wildcard IPv4 address on an {@code UP} non-virtual interface, or {@code null}
   * if none can be determined.
   */
  public static String scanRoutableIpv4() {
    try {
      java.util.Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics != null && nics.hasMoreElements()) {
        NetworkInterface nic = nics.nextElement();
        try {
          if (!nic.isUp() || nic.isLoopback() || nic.isVirtual()) {
            continue;
          }
        } catch (SocketException ignore) {
          continue;
        }
        java.util.Enumeration<InetAddress> addrs = nic.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress a = addrs.nextElement();
          if (isUnusableForAdvertise(a)) {
            continue;
          }
          if (a instanceof Inet4Address) {
            return a.getHostAddress();
          }
        }
      }
    } catch (Throwable t) {
      LOG.warn("scanRoutableIpv4() failed: " + t.getMessage());
    }
    return null;
  }

  private static boolean isUnusableForAdvertise(InetAddress a) {
    return a.isLoopbackAddress() || a.isLinkLocalAddress()
        || a.isAnyLocalAddress() || a.isMulticastAddress();
  }

  public static int chooseAListenPort(Configuration conf) throws IOException {
    PortReservation reservation = reserveListenPortPair(conf);
    // Historical behaviour: the caller only gets an int, so the reservation is dropped
    // immediately. Prefer #reserveListenPortPair when the caller can hold the reservation
    // until the real bind (see ParameterServerService / MatrixTransportServer).
    reservation.close();
    return reservation.getPort();
  }

  /**
   * Reserve a listen port pair ({@code port} and {@code port + 1}) by actually holding two
   * listening sockets on them.
   *
   * <p>Angel used to "choose" a port with {@link #isPortAvailable(int)}, i.e. test-bind then
   * immediately close, and only bind the real server later. That check-then-bind gap is a
   * race: several Angel processes on the same host draw from the same random port range, so
   * another process could grab the very port we just validated, and the real bind then dies
   * with {@code java.net.BindException: Address already in use}. The gap is especially wide
   * for the PS data port, which is {@code control port + 1} by protocol and is not bound
   * until {@code MatrixTransportServer#start()} - many seconds after selection.
   *
   * <p>Holding the sockets makes the reservation exclusive (SO_REUSEADDR is explicitly
   * disabled, so no other socket - wildcard or specific-address - can bind the same port
   * while we keep ours open). The caller releases each half right before the real server
   * binds it, shrinking the race window to microseconds.
   *
   * @return a held reservation, never null
   * @throws IOException if no free port pair could be found
   */
  public static PortReservation reserveListenPortPair(Configuration conf) throws IOException {
    int[] portRange = parsePortRange(conf);
    int startPort = portRange[0];
    int endPort = portRange[1];

    int maxTryTime = 100;
    Random r = new Random();

    for (int i = 0; i < maxTryTime; i++) {
      int port = Math.abs(r.nextInt()) % (endPort - startPort) + startPort;
      if (port + 1 > endPort) {
        continue;
      }

      ServerSocket controlHolder = holdPort(port);
      if (controlHolder == null) {
        LOG.error("workerservice:port " + port + " is not available, try agine");
        continue;
      }

      ServerSocket dataHolder = holdPort(port + 1);
      if (dataHolder == null) {
        closeQuietly(controlHolder);
        LOG.error("workerservice:port " + (port + 1) + " is not available, try agine");
        continue;
      }

      return new PortReservation(port, controlHolder, dataHolder);
    }

    throw new IOException(
      "can not find a avaliable port for workerservice in range [" + startPort + "," + endPort
        + "]");
  }

  private static int[] parsePortRange(Configuration conf) {
    String portRangeStr =
      conf.get(AngelConf.ANGEL_LISTEN_PORT_RANGE, AngelConf.DEFAULT_ANGEL_LISTEN_PORT_RANGE);

    String[] portRangeArray = null;
    int startPort = -1;
    int endPort = -1;
    try {
      portRangeArray = portRangeStr.split(",");
      startPort = Integer.valueOf(portRangeArray[0]);
      endPort = Integer.valueOf(portRangeArray[1]);

      if (startPort <= 1024 || startPort > 65535 || endPort <= 1024 || endPort > 65535
        || startPort > endPort) {
        throw new InvalidParameterException(AngelConf.ANGEL_LISTEN_PORT_RANGE, portRangeStr,
          "port should in range 1024~63335");
      }
    } catch (Exception x) {
      LOG.error("use port set for " + AngelConf.ANGEL_LISTEN_PORT_RANGE
        + " is unvalid, we use default value now. error msg = " + x.getMessage());
      portRangeArray = AngelConf.DEFAULT_ANGEL_LISTEN_PORT_RANGE.split(",");
      startPort = Integer.valueOf(portRangeArray[0]);
      endPort = Integer.valueOf(portRangeArray[1]);
    }

    return new int[] {startPort, endPort};
  }

  /**
   * Exclusively hold {@code port} on the wildcard address, or return null if it is taken.
   */
  private static ServerSocket holdPort(int port) {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket();
      // Exclusive: on Linux a wildcard and a specific-address socket may share a port only
      // when BOTH set SO_REUSEADDR, so disabling it here guarantees nobody else can bind.
      socket.setReuseAddress(false);
      socket.bind(new InetSocketAddress("0.0.0.0", port), 1);
      return socket;
    } catch (Exception e) {
      closeQuietly(socket);
      return null;
    }
  }

  private static void closeQuietly(ServerSocket socket) {
    if (socket != null) {
      try {
        socket.close();
      } catch (Exception e) {
        LOG.warn("close reserved socket failed: " + e.getMessage());
      }
    }
  }

  /**
   * Does this throwable (or any of its causes) mean "the address is already in use"?
   * Used to decide whether picking another port / retrying the bind can help.
   */
  public static boolean isAddressInUse(Throwable t) {
    Throwable cur = t;
    int depth = 0;
    while (cur != null && depth++ < 16) {
      if (cur instanceof BindException) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }

  /**
   * An exclusive reservation of {@code port} and {@code port + 1}, held by two real
   * listening sockets. Release each half right before the corresponding server binds it.
   */
  public static final class PortReservation implements Closeable {

    private final int port;
    private ServerSocket controlHolder;
    private ServerSocket dataHolder;

    private PortReservation(int port,ServerSocket controlHolder, ServerSocket dataHolder) {
      this.port = port;
      this.controlHolder = controlHolder;
      this.dataHolder = dataHolder;
    }

    /**
     * @return the reserved port, used by the control (rpc) server
     */
    public int getPort() {
      return port;
    }

    /**
     * @return the reserved {@code port + 1}, used by the PS matrix-transport (data) server
     */
    public int getDataPort() {
      return port + 1;
    }

    /**
     * Release the hold on {@link #getPort()}, to be called right before the real bind.
     */
    public synchronized void releaseControlPort() {
      closeQuietly(controlHolder);
      controlHolder = null;
    }

    /**
     * Release the hold on {@link #getDataPort()}, to be called right before the real bind.
     */
    public synchronized void releaseDataPort() {
      closeQuietly(dataHolder);
      dataHolder = null;
    }

    @Override
    public synchronized void close() {
      releaseControlPort();
      releaseDataPort();
    }
  }


  public static void testBindPort(String host, int port) throws IOException {
    Socket s = new Socket();
    try {
      s.bind(new InetSocketAddress(host, port));
    } finally {
      try {
        s.close();
      } catch (Exception x) {

      }
    }
  }

  public static boolean isPortAvailable(int port) {
    try {
      testBindPort("0.0.0.0", port);
      testBindPort(InetAddress.getLocalHost().getHostAddress(), port);
    } catch (Exception e) {
      return false;
    }

    try {
      testBindPort("0.0.0.0", port + 1);
      testBindPort(InetAddress.getLocalHost().getHostAddress(), port + 1);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
