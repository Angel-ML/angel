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
import com.tencent.angel.exception.AngelException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import py4j.GatewayServer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PythonRunner implements AppSubmitter {
  private static final Log LOG = LogFactory.getLog(PythonRunner.class);
  
  private static Configuration jconf;
  
  @Override
  public void submit(Configuration conf) throws Exception {
    jconf = conf;
    LOG.info("Launching PythonRunner ...");
    runPythonProcess();
  }
  
  public static Configuration getConf() {
    if (jconf != null) {
      return jconf;
    } else {
      return null;
    }
  }
  
  private void runPythonProcess() {
    String pythonFileName = jconf.get(AngelConf.PYANGEL_PYFILE);
    String pyFilename = jconf.get(AngelConf.PYANGEL_PYDEPFILES);
    Configuration conf = jconf;
    String pyAngelExec = conf.get(AngelConf.PYANGEL_PYTHON, "python");
  
    GatewayServer gatewayServer = new py4j.GatewayServer(null, 0);
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        gatewayServer.start();
      }
    });
    
    thread.setName("py4j-gateway-init");
    thread.setDaemon(true);
    thread.start();
    
    try {
      thread.join();
    } catch (InterruptedException ie) {
      LOG.error("failed to to start python server while join daemon thread: " + thread.getName(), ie);
      ie.printStackTrace();
    }
  
    // Create python path which include angel's jars, the python directory in ANGEL_HOME,
    // and other files submitted by user.
    ArrayList<String> pathItems = new ArrayList<>();
    pathItems.add(PythonUtils.getAngelPythonPath());
    pathItems.add(System.getenv("PYTHONPATH"));
    String pythonPath = String.join(File.separator, pathItems);
   
    LOG.info("python path is : " + pythonPath);
    
    // Launch python process
    List<String> pyProcInfoList = Arrays.asList(pyAngelExec, pythonFileName);
    ProcessBuilder pyProcBuilder = new ProcessBuilder(pyProcInfoList);
  
    Map<String, String> envMap = pyProcBuilder.environment();
    envMap.put("PYTHONPATH", pythonPath);
    envMap.put("PYTHONUNBUFFERED", "YES");
    envMap.put("PYANGEL_GATEWAY_PORT", "" + gatewayServer.getListeningPort());
    envMap.put("PYANGEL_PYTHON", conf.get(AngelConf.PYANGEL_PYTHON, "python"));
    envMap.put("PYTHONHASHSEED", System.getenv("PYTHONHASHSEED"));
    
    LOG.info("python gateway server port bind on : " + gatewayServer.getListeningPort());
    
    pyProcBuilder.redirectErrorStream(true);
    
    try {
      Process pyProc = pyProcBuilder.start();
      InputStream pyIns = pyProc.getInputStream();
      OutputStream pyOus = System.out;
      Thread redirectThread = new Thread(new Runnable() {
        @Override
        public void run() {
          byte[] buf = new byte[1024];
          try {
            int len = pyIns.read(buf);
            while (len != -1) {
              pyOus.write(buf, 0, len);
              pyOus.flush();
              len = pyIns.read(buf);
            }
          } catch (IOException ioe) {
            LOG.error("EOF: ", ioe);
          }
        }
      });
      
      redirectThread.start();
      
      int exitCode = pyProc.waitFor();
      if (exitCode != 0) {
        throw new AngelException("failed to start python process, the Error Code is: " + exitCode);
      }
    } catch (InterruptedException ie){
      LOG.error("failed to start redirect thread for python process", ie);
    } catch (IOException ioe) {
      LOG.error("EOF: ", ioe);
    } finally {
      gatewayServer.shutdown();
    }
  }
}
