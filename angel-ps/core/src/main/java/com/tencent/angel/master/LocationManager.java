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

package com.tencent.angel.master;

import com.tencent.angel.common.Location;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * parameter server and psagent location manager, it is used for workers to get locations of server
 */
public class LocationManager {
  private static final Log LOG = LogFactory.getLog(LocationManager.class);

  /**parameter server id to Location Builder map*/
  private final Map<ParameterServerId, LocationProto.Builder> psLocationMap;
  private final Lock psReadLock;
  private final Lock psWriteLock;
  
  /**psagent id to Location Builder map*/
  private final Map<PSAgentId, LocationProto.Builder> psAgentLocationMap;
  private final Lock psAgentReadLock;
  private final Lock psAgentWriteLock;
  
  private final AMContext context;

  public LocationManager(AMContext appContext) {
    psLocationMap = new HashMap<ParameterServerId, LocationProto.Builder>();
    ReadWriteLock psReadWriteLock = new ReentrantReadWriteLock();
    psReadLock = psReadWriteLock.readLock();
    psWriteLock = psReadWriteLock.writeLock();
    
    psAgentLocationMap = new HashMap<PSAgentId, LocationProto.Builder>();
    ReadWriteLock psAgentReadWriteLock = new ReentrantReadWriteLock();
    psAgentReadLock = psAgentReadWriteLock.readLock();
    psAgentWriteLock = psAgentReadWriteLock.writeLock();
    
    this.context = appContext;
  }

  /**
   * set location for a parameter server
   * @param psId parameter server id
   * @param location location(host and port)
   */
  public void setPSLocation(ParameterServerId psId, Location location) {
    psWriteLock.lock();
    try {
      if (location == null) {
        if (psLocationMap.containsKey(psId)) {
          LocationProto.Builder oldLocation = psLocationMap.remove(psId);
          LOG.warn("clear ps location for " + psId + ", oldLocation: "
              + locationBuilderToString(oldLocation));
        }
        return;
      }
      LocationProto.Builder psLoc = psLocationMap.get(psId);
      if (psLoc == null) {
        psLoc = LocationProto.newBuilder();
        LOG.info("set ps location, psId: " + psId + ", locaiton: " + location);
        psLocationMap.put(psId, psLoc);
      } else {
        LOG.info("update ps location, old location: " + locationBuilderToString(psLoc)
            + ", new location: " + location);
      }
      psLoc.setIp(location.getIp());
      psLoc.setPort(location.getPort());
    } finally {
      psWriteLock.unlock();
    }
  }

  /**
   * set location for a psagent
   * @param psAgentId psagent id
   * @param location location(host and port)
   */
  public void setPSAgentLocation(PSAgentId psAgentId, Location location) {
    psAgentWriteLock.lock();
    try {
      if (location == null) {
        if (psAgentLocationMap.containsKey(psAgentId)) {
          LocationProto.Builder oldLocation = psAgentLocationMap.remove(psAgentId);
          LOG.warn("clear psagent location for " + psAgentId + ", oldLocation: "
              + locationBuilderToString(oldLocation));
        }
        return;
      }
      LocationProto.Builder psLoc = psAgentLocationMap.get(psAgentId);
      if (psLoc == null) {
        psLoc = LocationProto.newBuilder();
        LOG.info("set psagent location, psAgentId: " + psAgentId + ", locaiton: " + location);
        psAgentLocationMap.put(psAgentId, psLoc);
      } else {
        LOG.info("update psagent location, old location: " + locationBuilderToString(psLoc)
            + ", new location: " + location);
      }
      psLoc.setIp(location.getIp());
      psLoc.setPort(location.getPort());
    } finally {
      psAgentWriteLock.unlock();
    }
  }

  /**
   * get all psagent location. 
   */
  public List<PSAgentLocation> getAllPSAgentLocation() {
    Set<PSAgentId> psAgentIds = context.getPSAgentManager().getPSAgentMap().keySet();
    psAgentReadLock.lock();
    List<PSAgentLocation> psLocList = new ArrayList<PSAgentLocation>(psAgentIds.size());
    try {
      PSAgentLocation.Builder psLocBuilder = PSAgentLocation.newBuilder();
      for (PSAgentId psAgentId : psAgentIds) {
        if (psLocationMap.containsKey(psAgentId)) {
          LOG.debug("psAgentId: " + psAgentId + ", location: "
              + locationBuilderToString(psAgentLocationMap.get(psAgentId)));
          psLocBuilder.setPsAgentStatus(PSAgentStatus.PSAGENT_OK);
          psLocBuilder.setPsAgentId(ProtobufUtil.convertToIdProto(psAgentId));
          psLocBuilder.setLocation(psAgentLocationMap.get(psAgentId).build());
        } else {
          LOG.debug("psAgentId: " + psAgentId + " location is not ready");
          psLocBuilder.setPsAgentStatus(PSAgentStatus.PSAGENT_NOTREADY);
          psLocBuilder.setPsAgentId(ProtobufUtil.convertToIdProto(psAgentId));
        }
        psLocList.add(psLocBuilder.build());
      }

      return psLocList;
    } finally {
      psAgentReadLock.unlock();
    }
  }

  /**
   * get all parameter server location. 
   */
  public List<PSLocation> getAllPSLocation() {
    Set<ParameterServerId> psIds =
        context.getParameterServerManager().getParameterServerMap().keySet();
    psReadLock.lock();
    List<PSLocation> psLocList = new ArrayList<PSLocation>(psIds.size());
    try {
      PSLocation.Builder psLocBuilder = PSLocation.newBuilder();
      for (ParameterServerId psId : psIds) {
        if (psLocationMap.containsKey(psId)) {
          LOG.debug("psId: " + psId + ", location: "
              + locationBuilderToString(psLocationMap.get(psId)));
          psLocBuilder.setPsStatus(PSStatus.PS_OK);
          psLocBuilder.setPsId(ProtobufUtil.convertToIdProto(psId));
          psLocBuilder.setLocation(psLocationMap.get(psId).build());
        } else {
          LOG.debug("psId: " + psId + " location is not ready");
          psLocBuilder.setPsStatus(PSStatus.PS_NOTREADY);
          psLocBuilder.setPsId(ProtobufUtil.convertToIdProto(psId));
        }
        psLocList.add(psLocBuilder.build());
      }

      return psLocList;
    } finally {
      psReadLock.unlock();
    }
  }

  private String locationBuilderToString(LocationProto.Builder locBuilder) {
    StringBuilder locStr = new StringBuilder();
    locStr.append(locBuilder.getIp()).append(":").append(locBuilder.getPort());
    return locStr.toString();
  }

  /**
   * get the location of a specific parameter server
   * @param psId parameter server id
   */
  public LocationProto getPSLocation(ParameterServerId psId) {
    psReadLock.lock();
    try {
      LocationProto.Builder builder = psLocationMap.get(psId);
      if (builder == null) {
        return null;
      }
      return psLocationMap.get(psId).build();
    } finally {
      psReadLock.unlock();
    }
  }
}
