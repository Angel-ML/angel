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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BuildProperties {
  public static String VERSION = "0.1";

  static {
    InputStream in = null;
    try {
      in = BuildProperties.class.getClassLoader().getResourceAsStream("build.properties");
      final Properties props = new Properties();
      props.load(in);
      VERSION = props.getProperty("version");
    } catch (final Throwable e) {

    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (final IOException e) {

        }
      }
    }
  }
}
