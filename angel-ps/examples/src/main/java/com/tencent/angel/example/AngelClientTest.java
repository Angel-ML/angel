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

package com.tencent.angel.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelPSClient;
import com.tencent.angel.client.yarn.AngelYarnClient;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.utils.AngelRunJar;

public class AngelClientTest {
  private static final Log LOG = LogFactory.getLog(AngelRunJar.class);
  private static final String angelSysConfFile = "angel-site.xml";
  private static final String parameterName1 = "w1";
  private static final String parameterName2 = "w2";
  public static final String matrixNamePrefix = "w_";

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
    String angelHomePath = System.getenv("ANGEL_HOME");
    if (angelHomePath == null) {
      LOG.fatal("ANGEL_HOME is empty, please set it first");
      throw new InvalidParameterException("ANGEL_HOME is empty, please set it first");
    }
    conf.addResource(new Path(angelHomePath + "/conf/" + angelSysConfFile));
    LOG.info("load system config file success");

    Map<String, String> cmdConfMap = parseArgs(args);
    for (Entry<String, String> kvEntry : cmdConfMap.entrySet()) {
      conf.set(kvEntry.getKey(), kvEntry.getValue());
    }

    AngelPSClient angelClient = new AngelPSClient(conf);
    angelClient.startPS(true);

    /*
     * int psNumber = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER,
     * AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
     * 
     * AngelYarnClient jobClient = new AngelYarnClient(conf); int maxDim =
     * conf.getInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM,
     * AngelConfiguration.DEFAULT_ANGEL_PREPROCESS_VECTOR_MAXDIM);
     * 
     * int matrixNum = conf.getInt("angel.test.matrix.number", 1); for (int i = 0; i < matrixNum;
     * i++) { MatrixContext mMatrix = new MatrixContext(); mMatrix.setName(matrixNamePrefix + i);
     * mMatrix.setRowNum(1); mMatrix.setColNum(maxDim); mMatrix.setMaxRowNumInBlock(1);
     * mMatrix.setMaxColNumInBlock(maxDim / psNumber);
     * mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
     * 
     * // mContext.set(ServerMatrixConf.SERVER_MATRIX_HOGWILD, "false");
     * mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
     * mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
     * 
     * jobClient.addMatrix(mMatrix); }
     * 
     * jobClient.submitApplication();
     */
  }

  private static Map<String, String> parseArgs(String[] args) throws InvalidParameterException {
    Map<String, String> kvMap = new HashMap<String, String>();

    int seg = 0;
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-D")) {
        String[] kv = args[i].substring(2).split("=");
        if (kv.length == 2) {
          kvMap.put(kv[0], kv[1]);
        } else {
          throw new InvalidParameterException("unvalid parameter " + args[i]);
        }
      } else if ((seg = args[i].indexOf(":")) > 0) {
        kvMap.put(args[i].substring(0, seg), args[i].substring(seg + 1));
      } else if (args[i].endsWith(".jar")) {

      } else {
        switch (args[i]) {
          case "jar": {
            if (i == args.length - 1) {
              throw new InvalidParameterException("there must be a jar file after jar commond");
            } else {
              i++;
              kvMap.put(AngelConfiguration.ANGEL_JOB_JAR, args[i]);
            }
            break;
          }
          default: {
            throw new InvalidParameterException("unvalid parameter " + args[i]);
          }
        }
      }
    }
    return kvMap;
  }
}
