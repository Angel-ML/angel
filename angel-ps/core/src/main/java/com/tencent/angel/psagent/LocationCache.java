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

package com.tencent.angel.psagent;

import com.tencent.angel.common.Location;
import com.tencent.angel.ps.ParameterServerId;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parameter server locations cache.
 */
public class LocationCache {
  /**master address*/
  private final Location masterLocation;
  
  /**server id to address map*/
  private final ConcurrentHashMap<ParameterServerId, Location> psLocations;
  
  /**all server ids*/
  private final ParameterServerId [] psIds;

  /**
   * 
   * Create a new LocationCache.
   *
   * @param masterLocation master address
   * @param psLocations ps addresses
   */
  public LocationCache(Location masterLocation, Map<ParameterServerId, Location> psLocations) {
    this.masterLocation = masterLocation;
    this.psIds = new ParameterServerId[psLocations.size()];
    this.psLocations = new ConcurrentHashMap<ParameterServerId, Location>(psLocations.size());
    
    int index = 0;
    for(Entry<ParameterServerId, Location> entry:psLocations.entrySet()) {
      psIds[index++] = entry.getKey();
      if(entry.getValue() != null) {
        this.psLocations.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Get master address.
   * 
   * @return Location master address
   */
  public Location getMasterLocation() {
    return masterLocation;
  }

  /**
   * Get the ps address.
   * 
   * @param id ps id
   * @return Location ps address
   */
  public Location getPSLocation(ParameterServerId id) {     
    return psLocations.get(id);
  }

  /**
   * Update ps address.
   * 
   * @param id ps id
   * @param loc ps address
   */
  public void setPSLocation(ParameterServerId id, Location loc) {     
    psLocations.put(id, loc);
  }

  /**
   * Get all ps ids.
   * 
   * @return  ParameterServerId[] ps ids
   */
  public ParameterServerId[] getPSIds() {
    return psIds;
  }
}
