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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.utils;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.InvalidParameterException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;

/**
 * Java class running tools for Angel.
 */
public class AngelRunJar {
  private static final Log LOG = LogFactory.getLog(AngelRunJar.class);
  private static final String angelSysConfFile = "angel-site.xml";

  public static void main(String[] args) {
    try{
      submit(ConfUtils.initConf(args));
    } catch (Exception x) {
      LOG.fatal("submit job failed ", x);
      System.exit(-1);
    }
  }

  public static void submit(Configuration conf) throws Exception {
    LOG.info("angel python file: " + conf.get("angel.pyangel.pyfile"));
    if (null != conf.get("angel.pyangel.pyfile")) {
      conf.set(AngelConf.ANGEL_APP_SUBMIT_CLASS, "com.tencent.angel.api.python.PythonRunner");
    }
    // instance submitter class
    final String submitClassName =
      conf.get(AngelConf.ANGEL_APP_SUBMIT_CLASS, AngelConf.DEFAULT_ANGEL_APP_SUBMIT_CLASS);
    UserGroupInformation ugi = UGITools.getCurrentUser(conf);
    ugi.doAs(new PrivilegedExceptionAction<String>() {
      @Override public String run() throws Exception {
        AppSubmitter submmiter = null;
        try {
          Class<?> submitClass = Class.forName(submitClassName);
          submmiter = (AppSubmitter) submitClass.newInstance();
          LOG.info("submitClass: " + submitClass.getName());
        } catch (Exception x) {
          String message = "load submit class failed " + x.getMessage();
          LOG.fatal(message, x);
          throw new InvalidParameterException(message);
        }

        submmiter.submit(conf);
        return "OK";
      }
    });
  }
}
