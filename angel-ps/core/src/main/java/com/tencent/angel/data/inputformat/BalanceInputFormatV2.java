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

package com.tencent.angel.data.inputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

/**
 * A simple InputFormat implementation that splits data as evenly as possible.
 */
public class BalanceInputFormatV2 extends CombineTextInputFormat {

  private final static String COMPRESS_FACTOR = "angel.data.file.compress.factor";
  private final static String SPLIT_NUMBER = "angel.data.split.num";

  class FileInfo {

    final FileStatus stat;
    final BlockLocation[] blockLocations;

    FileInfo(FileStatus stat, BlockLocation[] blockLocations) {
      this.stat = stat;
      this.blockLocations = blockLocations;
    }
  }

  class BlockInfo {

    final Path path;
    final long offset;
    final long length;
    final String[] hosts;
    final String[] racks;

    public BlockInfo(Path path, long offset, long length, String[] hosts, String[] racks) {
      this.path = path;
      this.offset = offset;
      this.length = length;
      this.hosts = hosts;
      this.racks = racks;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    int needSplitNum = conf.getInt(SPLIT_NUMBER, -1);
    long maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 1024 * 1024 * 1024);
    float compressFactor = conf.getFloat(COMPRESS_FACTOR, 1.0f);

    // all the files in input set
    List<FileStatus> stats = listStatus(job);
    List<InputSplit> splits = new ArrayList<>();
    if (stats.size() == 0) {
      return splits;
    }

    // Shuffle the file
    Collections.shuffle(stats);

    // Get the blocks for all files
    long totalFileSize = 0L;
    List<FileInfo> fileInfos = new ArrayList<>(stats.size());
    List<FileInfo> unSplittableFiles = new ArrayList<>();
    for (FileStatus status : stats) {
      FileInfo fileInfo = new FileInfo(status, getBlocks(status, conf));
      if (isSplitable(job, status.getPath())) {
        fileInfos.add(fileInfo);
      } else {
        unSplittableFiles.add(fileInfo);
      }
      totalFileSize += status.getLen();
    }

    if(needSplitNum <= 0) {
      needSplitNum = (int)(totalFileSize / maxSize);
    }

    // Adjust the maxSize to make the split more balanced
    maxSize = adjustMaxSize(totalFileSize, needSplitNum);


    if (fileInfos.isEmpty()) {
      compressFactor = 1.0f;
    }

    // Handle the splittable files
    handleSplittableFiles(splits, fileInfos, maxSize);

    // Handle the unsplittable files
    handleUnsplittableFiles(splits, unSplittableFiles, maxSize, compressFactor);

    return splits;
  }

  private long adjustMaxSize(long totalFileSize, int needSplitNum) {
    long a = totalFileSize / needSplitNum;
    long b = totalFileSize - a * needSplitNum;

    if(b == 0 || b > 0.98 * a){
      return a;
    }

    while(true) {
      long delta = (a - b) / (needSplitNum + 1);
      if(delta <= 0) {
        break;
      }
      a -= delta;
      b += needSplitNum * delta;
      if(b >= (long)(0.98 * a)) {
        break;
      }
    }
    return a;
  }

  private void handleSplittableFiles(List<InputSplit> splits, List<FileInfo> fileInfos,
      long maxSize)
      throws IOException {
    if (fileInfos.isEmpty()) {
      return;
    }

    // Split the blocks
    long currentSplitLen = 0L;
    long currentBlockOffset = 0L;
    List<BlockInfo> splitBlocks = new ArrayList<>();
    int fileNum = fileInfos.size();
    int blockNum;
    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      FileInfo fileInfo = fileInfos.get(fileIndex);

      blockNum = fileInfo.blockLocations.length;
      for (int blockIndex = 0; blockIndex < blockNum; ) {
        BlockLocation blockLocation = fileInfo.blockLocations[blockIndex];
        if (currentSplitLen + blockLocation.getLength() - currentBlockOffset <= maxSize) {
          // If the remaining size of the current block is smaller than the required size,
          // the remaining blocks are divided into the current split
          splitBlocks.add(new BlockInfo(fileInfo.stat.getPath(),
              blockLocation.getOffset() + currentBlockOffset,
              blockLocation.getLength() - currentBlockOffset, blockLocation.getHosts(), null));

          // Update current split length and move to next block
          currentSplitLen += (blockLocation.getLength() - currentBlockOffset);
          blockIndex++;

          // Clear the current block offset
          currentBlockOffset = 0;
        } else {
          // Current split length is > maxSize, split the block and generate a new split
          currentBlockOffset += maxSize - currentSplitLen;
          splitBlocks.add(new BlockInfo(fileInfo.stat.getPath(),
              blockLocation.getOffset() + currentBlockOffset, maxSize - currentSplitLen,
              blockLocation.getHosts(), null));
          splits.add(generateSplit(splitBlocks));

          // Clear blocks list for next split
          splitBlocks.clear();

          // Clear the current split length
          currentSplitLen = 0;
        }
      }
    }

    // If splitBlocks is not empty, just genetate a split for it
    if (!splitBlocks.isEmpty()) {
      splits.add(generateSplit(splitBlocks));
    }
  }

  private void handleUnsplittableFiles(List<InputSplit> splits, List<FileInfo> unSplittableFiles,
      long maxSize, float compressFactor)
      throws IOException {
    if (unSplittableFiles.isEmpty()) {
      return;
    }

    long totalFileSize = 0L;
    for (FileInfo fileInfo : unSplittableFiles) {
      totalFileSize = (long) (compressFactor * fileInfo.stat.getLen());
    }

    int needBlockNum = (int) (totalFileSize / maxSize) + 1;

    Map<Integer, List<BlockInfo>> splitSlots = new HashMap<>(needBlockNum);
    Map<Integer, Long> splitLens = new HashMap<>(needBlockNum);
    for (int i = 0; i < needBlockNum; i++) {
      splitSlots.put(i, new ArrayList<>());
      splitLens.put(i, 0L);
    }

    int fileNum = unSplittableFiles.size();
    int chooseIndex;
    FileInfo fileInfo;
    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      chooseIndex = choseMinLenIndex(splitLens);
      fileInfo = unSplittableFiles.get(fileIndex);
      splitSlots.get(chooseIndex)
          .add(new BlockInfo(fileInfo.stat.getPath(), 0, fileInfo.stat.getLen(),
              fileInfo.blockLocations[0].getHosts(), null));
      splitLens.put(chooseIndex, splitLens.get(chooseIndex) + fileInfo.stat.getLen());
    }

    for (Entry<Integer, List<BlockInfo>> splitEntry : splitSlots.entrySet()) {
      if (splitEntry.getValue().size() > 0) {
        splits.add(generateSplit(splitEntry.getValue()));
      }
    }
  }

  private int choseMinLenIndex(Map<Integer, Long> splitLens) {
    long minLen = Long.MAX_VALUE;
    int choosedIndex = 0;
    for (Entry<Integer, Long> entry : splitLens.entrySet()) {
      if (entry.getValue() < minLen) {
        minLen = entry.getValue();
        choosedIndex = entry.getKey();
      }
    }

    return choosedIndex;
  }

  private BlockLocation[] getBlocks(FileStatus stat, Configuration conf) throws IOException {
    // get block locations from file system
    if (stat instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) stat).getBlockLocations();
    } else {
      FileSystem fs = stat.getPath().getFileSystem(conf);
      return fs.getFileBlockLocations(stat, 0, stat.getLen());
    }
  }

  private InputSplit generateSplit(List<BlockInfo> blocks) {
    HashSet<String> locations = new HashSet<>();
    // create an input split
    Path[] fl = new Path[blocks.size()];
    long[] offset = new long[blocks.size()];
    long[] length = new long[blocks.size()];
    for (int i = 0; i < blocks.size(); i++) {
      fl[i] = blocks.get(i).path;
      offset[i] = blocks.get(i).offset;
      length[i] = blocks.get(i).length;
      for (int j = 0; j < blocks.get(i).hosts.length; j++) {
        locations.add(blocks.get(i).hosts[j]);
      }
    }

    return new CombineFileSplit(fl, offset, length, locations.toArray(new String[0]));
  }
}
