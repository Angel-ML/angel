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

package com.tencent.angel.example.quickStart;

//import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

/**
 * Created by payniexiao on 2017/8/21.
 */
public class TestString {
  public static void main(String [] args) {
    String [] paras = {};
    //paras[1] = "abc";
    //paras[2] = "123";
    for(String n:paras) {
      System.out.println(n);
    }

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, ",,1,2,3,4");
    // Add standard Hadoop classes
    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
      YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      System.out.println(c);;
    }
  }
}
