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

package com.tencent.angel.psagent.matrix;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.Map;

/**
 * PSAgent location manager
 */
public class PSAgentLocationManager {
  /**
   * Location manager
   */
  private final LocationManager locationManager;

  /**
   * PSAgent context
   */
  private final PSAgentContext context;

  /**
   * Create a PSAgentLocationManager
   * @param context PSAgent context
   */
  public PSAgentLocationManager(PSAgentContext context) {
    locationManager = new LocationManager();
    this.context = context;
  }

  /**
   * Get PS location
   * @param psId ps id
   * @return PS location
   */
  public Location getPsLocation(ParameterServerId psId) {
    return locationManager.getPsLocation(psId);
  }

  /**
   * Get PS location
   * @param psId ps id
   * @param sync true means get from Master, false means just get from local cache
   * @return ps location
   * @throws ServiceException
   */
  public Location getPsLocation(ParameterServerId psId, boolean sync) throws ServiceException {
    if(!sync) {
      return locationManager.getPsLocation(psId);
    } else {
      Location location = context.getMasterClient().getPSLocation(psId);
      setPsLocation(psId, location);
      return location;
    }
  }

  /**
   * Set Master location
   * @param location Master location
   */
  public void setMasterLocation(Location location) {
    locationManager.setMasterLocation(location);
  }

  /**
   * Set all PS ids
   * @param psIds all PS ids
   */
  public void setPsIds(ParameterServerId[] psIds) {
    locationManager.setPsIds(psIds);
  }

  /**
   * Set PS location
   * @param psId PS id
   * @param location PS location
   */
  public void setPsLocation(ParameterServerId psId, Location location) {
    locationManager.setPsLocation(psId, location);
  }

  /**
   * Get Master location
   * @return Master location
   */
  public Location getMasterLocation() {
    return locationManager.getMasterLocation();
  }

  /**
   * Get all PS ids
   * @return all PS ids
   */
  public ParameterServerId[] getPsIds() {
    return locationManager.getPsIds();
  }

  /**
   * Lookup ps id use location
   * @param loc ps location
   * @return ps id
   */
  public ParameterServerId getPsId(Location loc) {
    Map<ParameterServerId, Location> locations = locationManager.getPsLocations();
    for(Map.Entry<ParameterServerId, Location> locEntry : locations.entrySet()) {
      if(loc.equals(locEntry.getValue())) {
        return locEntry.getKey();
      }
    }
    return null;
  }
}
