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


package com.tencent.angel.io;

import java.net.InetSocketAddress;

/**
 * Utility for network addresses, resolving and naming.
 */
public class Addressing {
  public static final String VALID_PORT_REGEX = "[\\d]+";
  public static final String HOSTNAME_PORT_SEPARATOR = ":";

  /**
   * @param hostAndPort Formatted as <code>&lt;hostname> ':' &lt;port></code>
   * @return An InetSocketInstance
   */
  public static InetSocketAddress createInetSocketAddressFromHostAndPortStr(
    final String hostAndPort) {
    return new InetSocketAddress(parseHostname(hostAndPort), parsePort(hostAndPort));
  }

  /**
   * @param hostname Server hostname
   * @param port     Server port
   * @return Returns a concatenation of <code>hostname</code> and <code>port</code> in following
   * form: <code>&lt;hostname> ':' &lt;port></code>. For example, if hostname is
   * <code>example.org</code> and port is 1234, this method will return
   * <code>example.org:1234</code>
   */
  public static String createHostAndPortStr(final String hostname, final int port) {
    return hostname + HOSTNAME_PORT_SEPARATOR + port;
  }

  /**
   * @param hostAndPort Formatted as <code>&lt;hostname> ':' &lt;port></code>
   * @return The hostname portion of <code>hostAndPort</code>
   */
  public static String parseHostname(final String hostAndPort) {
    int colonIndex = hostAndPort.lastIndexOf(HOSTNAME_PORT_SEPARATOR);
    if (colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    return hostAndPort.substring(0, colonIndex);
  }

  /**
   * @param hostAndPort Formatted as <code>&lt;hostname> ':' &lt;port></code>
   * @return The port portion of <code>hostAndPort</code>
   */
  public static int parsePort(final String hostAndPort) {
    int colonIndex = hostAndPort.lastIndexOf(HOSTNAME_PORT_SEPARATOR);
    if (colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    return Integer.parseInt(hostAndPort.substring(colonIndex + 1));
  }
}
