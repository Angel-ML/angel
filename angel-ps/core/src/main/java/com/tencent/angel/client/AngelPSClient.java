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

package com.tencent.angel.client;

import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ml.matrix.MatrixContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A simple angel client used for a third party computing system. Angel support ps service running
 * mode, which can provide ParameterServer service for any third party computing systems.
 */
public class AngelPSClient {
  private static final Log LOG = LogFactory.getLog(AngelPSClient.class);
  private static final String angelSysConfFile = "angel-site.xml";
  private final Configuration conf;
  private final AngelClient client;

  public AngelPSClient() throws InvalidParameterException {
    conf = new Configuration();
    loadSysConfig();
    client = AngelClientFactory.get(conf);
  }

  public AngelPSClient(Configuration conf) throws InvalidParameterException {
    this.conf = conf;
    client = AngelClientFactory.get(conf);
  }

  private void loadSysConfig() throws InvalidParameterException {
    String angelHomePath = System.getenv("ANGEL_HOME");
    if (angelHomePath == null) {
      LOG.fatal("ANGEL_HOME is empty, please set it first");
      throw new InvalidParameterException("ANGEL_HOME is empty, please set it first");
    }
    conf.addResource(new Path(angelHomePath + "/conf/" + angelSysConfFile));
    LOG.info("load system config file success");
  }

  public AngelPSClient(PSStartUpConfig config) throws InvalidParameterException {
    conf = new Configuration();
    loadSysConfig();

    conf.setInt(AngelConf.ANGEL_PS_NUMBER, config.getPsNum());
    conf.setInt(AngelConf.ANGEL_PS_MEMORY_MB, config.getPsMemoryInMB());
    conf.setInt(AngelConf.ANGEL_PS_CPU_VCORES, config.getPsVcoreNum());

    conf.setInt(AngelConf.ANGEL_PSAGENT_NUMBER, config.getPsAgentNum());
    conf.setInt(AngelConf.ANGEL_PSAGENT_MERMORY_MB, config.getPsAgentMemoryInMB());
    conf.setInt(AngelConf.ANGEL_PSAGENT_CPU_VCORES, config.getPsAgentVcoreNum());

    Map<String, String> paramsMap = config.getConfig();
    if (paramsMap != null && !paramsMap.isEmpty()) {
      for (Entry<String, String> entry : paramsMap.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    client = AngelClientFactory.get(conf);
  }

  /**
   * Start Angel ps
   * @return Angel ps running context
   * @throws AngelException
   */
  public AngelContext startPS() throws AngelException  {
    int psNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    if (psNum <= 0) {
      throw new AngelException("Invalid parameter:Wrong ps number!");
    }
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString());
    client.addMatrix(new MatrixContext("init", 1, psNum, 1, 1));
    client.startPSServer();
    client.run(); 
    return new AngelContext(client.getMasterLocation(), conf);
  }

  /**
   * Add a matrix
   * @param mContext matrix context
   */
  public void addMatrix(MatrixContext mContext)  throws AngelException{
    client.addMatrix(mContext);
  }

  /**
   * Save given matrices
   * @param matrixNames need to save matrix name list
   * @throws AngelException
   */
  public void save(List<String> matrixNames) throws AngelException{
    client.saveMatrices(matrixNames);
  }

  /**
   * Stop Angel ps
   */
  public void stopPS(){
    try {
      client.stop(0);
    } catch (AngelException e) {
      LOG.warn("stop ps failed.", e);
    }
  }

  /**
   * Kill Angel ps
   */
  public void killPS(){
    try {
      client.stop();
    } catch (AngelException e) {
      LOG.warn("kill ps failed.", e);
    }
  }
}
