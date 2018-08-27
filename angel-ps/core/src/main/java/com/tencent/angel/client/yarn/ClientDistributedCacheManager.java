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


package com.tencent.angel.client.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ClientDistributedCacheManager {

  public void addResource(FileSystem fs, Configuration conf, Path destPath,
    Map<String, LocalResource> localResources, LocalResourceType resourceType, String link,
    Map<URI, FileStatus> statCache, boolean appMasterOnly) throws IOException {

    FileStatus destStatus = fs.getFileStatus(destPath);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    amJarRsrc.setType(resourceType);

    LocalResourceVisibility visibility = getVisibility(conf, destPath.toUri(), statCache);
    amJarRsrc.setVisibility(visibility);
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    amJarRsrc.setTimestamp(destStatus.getModificationTime());
    amJarRsrc.setSize(destStatus.getLen());

    if (link == null || link.isEmpty())
      throw new IOException("You must specify a valid link name");

    localResources.put(link, amJarRsrc);
  }

  public LocalResourceVisibility getVisibility(Configuration conf, URI uri,
    Map<URI, FileStatus> statCache) throws IOException {
    if (isPublic(conf, uri, statCache)) {
      return LocalResourceVisibility.PUBLIC;
    } else {
      return LocalResourceVisibility.PRIVATE;
    }
  }

  public boolean isPublic(Configuration conf, URI uri, Map<URI, FileStatus> statCache)
    throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path current = new Path(uri.getPath());
    // the leaf level file should be readable by others
    if (!checkPermissionOfOther(fs, current, FsAction.READ, statCache)) {
      return false;
    }
    return ancestorsHaveExecutePermissions(fs, current.getParent(), statCache);
  }

  public boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path,
    Map<URI, FileStatus> statCache) throws IOException {
    Path current = path;
    while (current != null) {
      // the subdirs in the path should have execute permissions for
      // others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
        return false;
      }
      current = current.getParent();
    }
    return true;
  }

  /**
   * Checks for a given path whether the Other permissions on it imply the permission in the passed
   * FsAction
   *
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  public boolean checkPermissionOfOther(FileSystem fs, Path path, FsAction action,
    Map<URI, FileStatus> statCache) throws IOException {
    FileStatus status = getFileStatus(fs, path.toUri(), statCache);
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    return otherAction.implies(action);
  }

  public FileStatus getFileStatus(FileSystem fs, URI uri, Map<URI, FileStatus> statCache)
    throws IOException {
    FileStatus fstat = statCache.get(uri);
    if (fstat == null) {
      fstat = fs.getFileStatus(new Path(uri));
      statCache.put(uri, fstat);
    }

    return fstat;
  }
}
