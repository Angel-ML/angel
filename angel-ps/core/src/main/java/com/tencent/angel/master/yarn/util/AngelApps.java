/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.master.yarn.util;

import com.tencent.angel.conf.AngelConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for Angel applications
 */
public class AngelApps extends Apps {
  public static final Log LOG = LogFactory.getLog(AngelApps.class);

  @SuppressWarnings("deprecation")
  public static void setClasspath(Map<String, String> environment, Configuration conf)
      throws IOException {
    String classpathEnvVar = Environment.CLASSPATH.name();
    Apps.addToEnvironment(environment, classpathEnvVar, Environment.PWD.$());
    Apps.addToEnvironment(environment, classpathEnvVar, Environment.PWD.$() + Path.SEPARATOR + "*");
    // a * in the classpath will only find a .jar, so we need to filter out
    // all .jars and add everything else
    addToClasspathIfNotJar(DistributedCache.getFileClassPaths(conf),
        DistributedCache.getCacheFiles(conf), conf, environment, classpathEnvVar);
    addToClasspathIfNotJar(DistributedCache.getArchiveClassPaths(conf),
        DistributedCache.getCacheArchives(conf), conf, environment, classpathEnvVar);
    
    AngelApps.setAngelFrameworkClasspath(environment, conf);
  }

  private static void setAngelFrameworkClasspath(Map<String, String> environment, Configuration conf)
      throws IOException {
    // Propagate the system classpath when using the mini cluster
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          System.getProperty("java.class.path"));
    }

    // Add standard Hadoop classes
    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c.trim());
    }
    for (String c : conf.getStrings(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH))) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c.trim());
    }
  }

  private static void addToClasspathIfNotJar(Path[] paths, URI[] withLinks, Configuration conf,
      Map<String, String> environment, String classpathEnvVar) throws IOException {
    if (paths != null) {
      HashMap<Path, String> linkLookup = new HashMap<Path, String>();
      if (withLinks != null) {
        for (URI u : withLinks) {
          Path p = new Path(u);
          FileSystem remoteFS = p.getFileSystem(conf);
          p =
              remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
                  remoteFS.getWorkingDirectory()));
          String name = (null == u.getFragment()) ? p.getName() : u.getFragment();
          if (!name.toLowerCase().endsWith(".jar")) {
            linkLookup.put(p, name);
          }
        }
      }

      for (Path p : paths) {
        FileSystem remoteFS = p.getFileSystem(conf);
        p =
            remoteFS
                .resolvePath(p.makeQualified(remoteFS.getUri(), remoteFS.getWorkingDirectory()));
        String name = linkLookup.get(p);
        if (name == null) {
          name = p.getName();
        }
        if (!name.toLowerCase().endsWith(".jar")) {
          Apps.addToEnvironment(environment, classpathEnvVar, Environment.PWD.$() + Path.SEPARATOR
              + name);
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  public static void setupDistributedCache(Configuration conf,
      Map<String, LocalResource> localResources) throws IOException {

    // Cache archives
    parseDistributedCacheArtifacts(conf, localResources, LocalResourceType.ARCHIVE,
        DistributedCache.getCacheArchives(conf), DistributedCache.getArchiveTimestamps(conf),
        getFileSizes(conf, MRJobConfig.CACHE_ARCHIVES_SIZES),
        DistributedCache.getArchiveVisibilities(conf));

    // Cache files
    parseDistributedCacheArtifacts(conf, localResources, LocalResourceType.FILE,
        DistributedCache.getCacheFiles(conf), DistributedCache.getFileTimestamps(conf),
        getFileSizes(conf, MRJobConfig.CACHE_FILES_SIZES),
        DistributedCache.getFileVisibilities(conf));
  }

  private static void parseDistributedCacheArtifacts(Configuration conf,
      Map<String, LocalResource> localResources, LocalResourceType type, URI[] uris,
      long[] timestamps, long[] sizes, boolean visibilities[]) throws IOException {

    if (uris != null) {
      // Sanity check
      if ((uris.length != timestamps.length) || (uris.length != sizes.length)
          || (uris.length != visibilities.length)) {
        throw new IllegalArgumentException("Invalid specification for "
            + "distributed-cache artifacts of type " + type + " :" + " #uris=" + uris.length
            + " #timestamps=" + timestamps.length + " #visibilities=" + visibilities.length);
      }

      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u);
        FileSystem remoteFS = p.getFileSystem(conf);
        p =
            remoteFS
                .resolvePath(p.makeQualified(remoteFS.getUri(), remoteFS.getWorkingDirectory()));
        // Add URI fragment or just the filename
        Path name = new Path((null == u.getFragment()) ? p.getName() : u.getFragment());
        if (name.isAbsolute()) {
          throw new IllegalArgumentException("Resource name must be relative");
        }
        String linkName = name.toUri().getPath();
        LocalResource orig = localResources.get(linkName);
        org.apache.hadoop.yarn.api.records.URL url = ConverterUtils.getYarnUrlFromURI(p.toUri());
        if (orig != null && !orig.getResource().equals(url)) {
          LOG.warn(getResourceDescription(orig.getType()) + toString(orig.getResource())
              + " conflicts with " + getResourceDescription(type) + toString(url)
              + " This will be an error in Hadoop 2.0");
          continue;
        }
        localResources.put(linkName, LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(p
            .toUri()), type, visibilities[i] ? LocalResourceVisibility.PUBLIC
            : LocalResourceVisibility.PRIVATE, sizes[i], timestamps[i]));
      }
    }
  }

  private static String getResourceDescription(LocalResourceType type) {
    if (type == LocalResourceType.ARCHIVE || type == LocalResourceType.PATTERN) {
      return "cache archive (" + MRJobConfig.CACHE_ARCHIVES + ") ";
    }
    return "cache file (" + MRJobConfig.CACHE_FILES + ") ";
  }

  private static String toString(org.apache.hadoop.yarn.api.records.URL url) {
    StringBuilder b = new StringBuilder();
    b.append(url.getScheme()).append("://").append(url.getHost());
    if (url.getPort() >= 0) {
      b.append(":").append(url.getPort());
    }
    b.append(url.getFile());
    return b.toString();
  }

  private static long[] getFileSizes(Configuration conf, String key) {
    String[] strs = conf.getStrings(key);
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for (int i = 0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }

  private static String STAGING_CONSTANT = ".staging";

  public static Path getStagingDir(Configuration conf, String user) {
    return new Path(conf.get(AngelConf.ANGEL_STAGING_DIR,
        AngelConf.DEFAULT_ANGEL_STAGING_DIR)
        + Path.SEPARATOR
        + user
        + Path.SEPARATOR
        + STAGING_CONSTANT);
  }

  public static void addLog4jSystemProperties(String logLevel, long logSize, List<String> vargs) {
    vargs.add("-Dlog4j.configuration=log/angel.properties");
    vargs.add("-Dlog4j.logger.com.tencent.ml=DEBUG");
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=" + logSize);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA");
  }
}
