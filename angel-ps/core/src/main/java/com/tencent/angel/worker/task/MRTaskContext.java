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

package com.tencent.angel.worker.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;

/**
 * Simple wrapper of {@link TaskAttemptContext} for {@link RecordReader#initialize(InputSplit, TaskAttemptContext)}
 */
public class MRTaskContext implements TaskAttemptContext {

  private Configuration conf;

  public MRTaskContext(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Path[] getArchiveClassPaths() {
        return null;
  }

  @Override
  public String[] getArchiveTimestamps() {
        return null;
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
        return null;
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
        return null;
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        return null;
  }

  @Override
  public Configuration getConfiguration() {
        return conf;
  }

  @Override
  public Credentials getCredentials() {
        return null;
  }

  @Override
  public Path[] getFileClassPaths() {
        return null;
  }

  @Override
  public String[] getFileTimestamps() {
        return null;
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
        return null;
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        return null;
  }

  @Override
  public String getJar() {
        return null;
  }

  @Override
  public JobID getJobID() {
        return null;
  }

  @Override
  public String getJobName() {
        return null;
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
        return false;
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
        return null;
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
        return null;
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
        return null;
  }

  @Override
  public Class<?> getMapOutputValueClass() {
        return null;
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        return null;
  }

  @Override
  public int getMaxMapAttempts() {
        return 0;
  }

  @Override
  public int getMaxReduceAttempts() {
        return 0;
  }

  @Override
  public int getNumReduceTasks() {
        return 0;
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        return null;
  }

  @Override
  public Class<?> getOutputKeyClass() {
        return null;
  }

  @Override
  public Class<?> getOutputValueClass() {
        return null;
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        return null;
  }

  @Override
  public boolean getProfileEnabled() {
        return false;
  }

  @Override
  public String getProfileParams() {
        return null;
  }

  @Override
  public IntegerRanges getProfileTaskRange(boolean arg0) {
        return null;
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        return null;
  }

  @Override
  public RawComparator<?> getSortComparator() {
        return null;
  }

  @Override
  public boolean getSymlink() {
        return false;
  }

  @Override
  public boolean getTaskCleanupNeeded() {
        return false;
  }

  @Override
  public String getUser() {
        return null;
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
        return null;
  }

  @Override
  public void progress() {
    
  }

  @Override
  public Counter getCounter(Enum<?> arg0) {
        return null;
  }

  @Override
  public Counter getCounter(String arg0, String arg1) {
        return null;
  }

  @Override
  public float getProgress() {
        return 0;
  }

  @Override
  public String getStatus() {
        return null;
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
        return null;
  }

  @Override
  public void setStatus(String arg0) {
    
  }

  public RawComparator<?> getCombinerKeyGroupingComparator() {
        return null;
  }

}
