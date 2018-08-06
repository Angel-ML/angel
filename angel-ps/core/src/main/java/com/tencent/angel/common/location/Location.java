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

package com.tencent.angel.common.location;

import com.tencent.angel.common.Serialize;

/**
 * Location of Angel Master/PS/Worker.
 */
public class Location {
  /**ip address*/
  private final String ip;
  
  /**listening port*/
  private final int port;

  /**
   * Create a new location
   * @param ip ip address
   * @param port listening port
   */
  public Location(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  /**
   * Get ip
   * @return ip
   */
  public String getIp() {
    return ip;
  }

  /**
   * Get listening port
   * @return listening port
   */
  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return "(" + ip + ":" + port + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ip == null) ? 0 : ip.hashCode());
    result = prime * result + port;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Location other = (Location) obj;
    if (ip == null) {
      if (other.ip != null)
        return false;
    } else if (!ip.equals(other.ip))
      return false;
    return port == other.port;
  }
}
