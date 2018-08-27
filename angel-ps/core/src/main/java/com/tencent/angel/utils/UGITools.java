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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * Hadoop ugi modify tool.
 */
public class UGITools {
  private static final Log LOG = LogFactory.getLog(UGITools.class);
  final static public String UGI_PROPERTY_NAME = "hadoop.job.ugi";

  public static UserGroupInformation getCurrentUser(Configuration conf)
    throws IOException, ClassNotFoundException, NoSuchFieldException, SecurityException,
    InstantiationException, IllegalAccessException {
    String[] ugiStrs = conf.getStrings(UGI_PROPERTY_NAME);
    if (ugiStrs == null) {
      LOG.info("UGI_PROPERTY_NAME is null ");
      return UserGroupInformation.getCurrentUser();
    } else {
      String[] userPass = ugiStrs[0].split(":");
      Class<?> ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
      Class<?> userClass = Class.forName("org.apache.hadoop.security.User");
      Field userFiled = ugiClass.getDeclaredField("user");
      userFiled.setAccessible(true);

      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      Field shortNameFiled = userClass.getDeclaredField("shortName");
      Field fullNameFiled = userClass.getDeclaredField("fullName");

      shortNameFiled.setAccessible(true);
      fullNameFiled.setAccessible(true);
      shortNameFiled.set(userFiled.get(ugi), userPass[0]);
      fullNameFiled.set(userFiled.get(ugi), userPass[0]);

      return ugi;
    }
  }

  public static void main(String[] args)
    throws ClassNotFoundException, InstantiationException, IllegalAccessException,
    NoSuchFieldException, SecurityException, IllegalArgumentException, InvocationTargetException,
    NoSuchMethodException, IOException {
    Configuration conf = new Configuration();
    conf.set(UGI_PROPERTY_NAME, "abc:123456");
    UserGroupInformation ugi = getCurrentUser(conf);
    LOG.info(ugi.getUserName());
  }
}
