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

import com.tencent.angel.common.location.Location;
import org.apache.hadoop.conf.Configuration;

/**
 * Angel application running context.
 */
public class AngelContext {
  /**master location*/
  private final Location masterLocation;
  
  /**application configuration*/
  private final Configuration conf;
  
  /**
   * 
   * Create a new AngelContext.
   *
   * @param masterLocation master location
   * @param conf application configuration
   */
  public AngelContext(Location masterLocation, Configuration conf){
    this.masterLocation = masterLocation;
    this.conf = conf;
  }

  /**
   * Get application configuration.
   * 
   * @return  Configuration application configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the location of the application master.
   * 
   * @return Location the location of the application master
   */
  public Location getMasterLocation() {
    return masterLocation;
  }
}
