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


package com.tencent.angel.client;

import java.util.HashMap;
import java.util.Iterator;

import com.tencent.angel.api.python.PythonUtils;
import com.tencent.angel.client.kubernetes.AngelKubernetesClient;
import org.apache.hadoop.conf.Configuration;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.client.local.AngelLocalClient;
import com.tencent.angel.client.yarn.AngelYarnClient;
import com.tencent.angel.conf.AngelConf;

/**
 * Angel client factory, it support two types client now: LOCAL and YARN
 */
public class AngelClientFactory {

  private static AngelClientFactory factory;

  // Used for java code to get a AngelClient instance
  public static AngelClient get(Configuration conf) {
    if (factory == null) {
      factory = new AngelClientFactory();
    }
    return factory.doGet(conf);
  }

  // Used for python code to get a AngelClient instance
  public static AngelClient get(HashMap confMap, Configuration conf) {
    Configuration mapConf = PythonUtils.addMapToConf(confMap, conf);
    return get(mapConf);
  }

  public AngelClient doGet(Configuration conf) {
    String mode = conf.get(AngelConf.ANGEL_DEPLOY_MODE, AngelConf.DEFAULT_ANGEL_DEPLOY_MODE);

    if (mode.equals(AngelDeployMode.LOCAL.toString())) {
      return new AngelLocalClient(conf);
    } else if(mode.equals(AngelDeployMode.KUBERNETES.toString())) {
      return new AngelKubernetesClient(conf);
    } else {
      return new AngelYarnClient(conf);
    }
  }
}