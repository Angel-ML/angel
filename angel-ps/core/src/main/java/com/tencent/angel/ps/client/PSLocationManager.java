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

package com.tencent.angel.ps.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.PSContext;

/**
 * PS location manager
 */
public class PSLocationManager {
  /**
   * Location manager
   */
  private final LocationManager locationManager;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Create a PSLocationManager
   * @param context PS context
   */
  public PSLocationManager(PSContext context) {
    this.context = context;
    locationManager = new LocationManager();
  }

  /**
   * Get master location
   * @return master location
   */
  public Location getMasterLocation() {
    return locationManager.getMasterLocation();
  }

  /**
   * Set master location
   * @param masterLocation master location
   */
  public void setMasterLocation(Location masterLocation) {
    locationManager.setMasterLocation(masterLocation);
  }

  /**
   * Get ps location, it will first get from cache, if can not find it in cache, it will get from master
   * @param psId ps id
   * @return ps location, null if not found
   * @throws ServiceException
   */
  public Location getPsLocation(ParameterServerId psId) throws ServiceException {
    Location location = locationManager.getPsLocation(psId);
    if(location == null) {
      location = context.getMaster().getPsLocation(psId);
      if(location != null) {
        locationManager.setPsLocation(psId, location);
      }
    }
    return location;
  }
}
