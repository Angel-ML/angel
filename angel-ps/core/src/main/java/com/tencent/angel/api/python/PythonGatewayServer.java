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

package com.tencent.angel.api.python;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.conf.AngelConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;
import py4j.GatewayServer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Process that used to start Py4J GatewayServer, so that the python process can
 * call back to JVM via the port specified by the caller.
 * Currently we do not suggest you to use this Python API in some certain high
 * performance situation, for the reason that Py4J communicate JVM and python use
 * socket
 *
 * This process is launched via AngelRunJar by java_gateway.py
 */
public class PythonGatewayServer implements AppSubmitter {
  static {
    String currentPath = System.getenv("ANGEL_HOME");
    PropertyConfigurator.configure(currentPath + "/conf/log4j.properties");
  }
  private static final Log LOG = LogFactory.getLog(PythonGatewayServer.class);
  
  // Configuration that should be used in python environment, there should only be one
  // configuration instance in each Angel context.
  // Use private access means jconf should not be changed or modified in this way.
  private static Configuration jconf;
  
  @Override
  public void submit(Configuration conf) throws Exception {
    LOG.info("Starting python gateway server...");
    jconf = conf;
    startServer();
  }
 
  public static Configuration getJconf() {
    if (jconf != null) {
      return jconf;
    } else if (PythonRunner.getConf() != null) {
      return PythonRunner.getConf();
    } else {
      return null;
    }
  }
  
  private void startServer() throws IOException {
    GatewayServer gatewayServer = new GatewayServer(null, 0);
    gatewayServer.start();
    int boundPort = gatewayServer.getListeningPort();
    if (boundPort == -1) {
      LOG.error("GatewayServer failed to binding");
      System.exit(1);
    } else {
      LOG.debug("GatewayServer started on port: " + boundPort);
    }

    String callbackHost = System.getenv("_PYANGEL_CALLBACK_HOST");
    LOG.info("GatewayServer Host: " + callbackHost);
    int callbackPort = Integer.parseInt(System.getenv("_PYANGEL_CALLBACK_PORT"));
    LOG.info("GatewayServer Port: " + callbackPort);

    try {
      Socket callbackSocket = new Socket(callbackHost, callbackPort);
      DataOutputStream dos = new DataOutputStream(callbackSocket.getOutputStream());
      dos.writeInt(boundPort);
      dos.close();
      callbackSocket.close();

      // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
      while (System.in.read() != -1) {
        // Do nothing
      }
    } catch (IOException ioe) {
      LOG.error("failed to start callback server: ", ioe);
    }
    LOG.debug("Exiting due to broken pipe from Python");
    System.exit(0);
  }
}
