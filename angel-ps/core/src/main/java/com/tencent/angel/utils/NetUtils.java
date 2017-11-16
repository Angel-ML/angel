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

package com.tencent.angel.utils;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.InvalidParameterException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.net.SocketFactory;
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
    if (listenAddr.isUnresolved() || listenAddr.getAddress().isAnyLocalAddress()
        || listenAddr.getAddress().isLoopbackAddress()) {
      ret = new InetSocketAddress(InetAddress.getLocalHost(), listenAddr.getPort());
    } else {
      ret = listenAddr;
    }

    return ret;
  }

  public static String getRealLocalIP() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostAddress();
  }

  public static int chooseAListenPort(Configuration conf) throws IOException {
    String portRangeStr =
        conf.get(AngelConf.ANGEL_LISTEN_PORT_RANGE,
            AngelConf.DEFAULT_ANGEL_LISTEN_PORT_RANGE);

    String[] portRangeArray = null;
    int startPort = -1;
    int endPort = -1;
    try {
      portRangeArray = portRangeStr.split(",");
      startPort = Integer.valueOf(portRangeArray[0]);
      endPort = Integer.valueOf(portRangeArray[1]);

      if (startPort <= 1024 || startPort > 65535 || endPort <= 1024 || endPort > 65535
          || startPort > endPort) {
        throw new InvalidParameterException(AngelConf.ANGEL_LISTEN_PORT_RANGE,
            portRangeStr, "port should in range 1024~63335");
      }
    } catch (Exception x) {
      LOG.error("use port set for " + AngelConf.ANGEL_LISTEN_PORT_RANGE
          + " is unvalid, we use default value now. error msg = " + x.getMessage());
      portRangeArray = AngelConf.DEFAULT_ANGEL_LISTEN_PORT_RANGE.split(",");
      startPort = Integer.valueOf(portRangeArray[0]);
      endPort = Integer.valueOf(portRangeArray[1]);
    }

    int maxTryTime = 100;
    Random r = new Random();
    int port = -1;

    for (int i = 0; i < maxTryTime; i++) {
      port = Math.abs(r.nextInt()) % (endPort - startPort) + startPort;
      if (isPortAvailable(port)) {
        return port;
      }

      LOG.error("workerservice:port " + port + " is not available, try agine");
    }

    throw new IOException("can not find a avaliable port for workerservice");
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
