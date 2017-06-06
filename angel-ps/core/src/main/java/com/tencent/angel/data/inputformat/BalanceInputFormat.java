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

/**
 * Add some balance policy to make the data block sizes are as equal as possible.
 */

package com.tencent.angel.data.inputformat;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.*;

public class BalanceInputFormat extends CombineTextInputFormat {

  private HashMap<String, Set<String>> rackToNodes =
    new HashMap<String, Set<String>>();

  static class OneFileInfo {
    private long fileSize;               // size of the file
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(FileStatus stat, Configuration conf,
      boolean isSplitable,
      HashMap<String, List<OneBlockInfo>> rackToBlocks,
      HashMap<OneBlockInfo, String[]> blockToNodes,
      HashMap<String, Set<OneBlockInfo>> nodeToBlocks,
      HashMap<String, Set<String>> rackToNodes,
      long maxSize)
      throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      BlockLocation[] locations;
      if (stat instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) stat).getBlockLocations();
      } else {
        FileSystem fs = stat.getPath().getFileSystem(conf);
        locations = fs.getFileBlockLocations(stat, 0, stat.getLen());
      }
      // create a list of all block and their locations
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else {

        if(locations.length == 0) {
          locations = new BlockLocation[] { new BlockLocation() };
        }

        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          blocks = new OneBlockInfo[1];
          fileSize = stat.getLen();
          blocks[0] = new OneBlockInfo(stat.getPath(), 0, fileSize,
            locations[0].getHosts(), locations[0].getTopologyPaths());
        } else {
          ArrayList<OneBlockInfo> blocksList = new ArrayList<OneBlockInfo>(
            locations.length);
          for (int i = 0; i < locations.length; i++) {
            fileSize += locations[i].getLength();

            // each split can be a maximum of maxSize
            long left = locations[i].getLength();
            long myOffset = locations[i].getOffset();
            long myLength = 0;
            do {
              if (maxSize == 0) {
                myLength = left;
              } else {
                if (left > maxSize && left < 2 * maxSize) {
                  // if remainder is between max and 2*max - then
                  // instead of creating splits of size max, left-max we
                  // create splits of size left/2 and left/2. This is
                  // a heuristic to avoid creating really really small
                  // splits.
                  myLength = left / 2;
                } else {
                  myLength = Math.min(maxSize, left);
                }
              }
              OneBlockInfo oneblock = new OneBlockInfo(stat.getPath(),
                myOffset, myLength, locations[i].getHosts(),
                locations[i].getTopologyPaths());
              left -= myLength;
              myOffset += myLength;

              blocksList.add(oneblock);
            } while (left > 0);
          }
          blocks = blocksList.toArray(new OneBlockInfo[blocksList.size()]);
        }

        populateBlockInfo(blocks, rackToBlocks, blockToNodes,
          nodeToBlocks, rackToNodes);
      }
    }

    static void populateBlockInfo(OneBlockInfo[] blocks,
      Map<String, List<OneBlockInfo>> rackToBlocks,
      Map<OneBlockInfo, String[]> blockToNodes,
      Map<String, Set<OneBlockInfo>> nodeToBlocks,
      Map<String, Set<String>> rackToNodes) {
      for (OneBlockInfo oneblock : blocks) {
        // add this block to the block --> node locations map
        blockToNodes.put(oneblock, oneblock.hosts);

        // For blocks that do not have host/rack information,
        // assign to default  rack.
        String[] racks = null;
        if (oneblock.hosts.length == 0) {
          racks = new String[]{NetworkTopology.DEFAULT_RACK};
        } else {
          racks = oneblock.racks;
        }

        // add this block to the rack --> block map
        for (int j = 0; j < racks.length; j++) {
          String rack = racks[j];
          List<OneBlockInfo> blklist = rackToBlocks.get(rack);
          if (blklist == null) {
            blklist = new ArrayList<OneBlockInfo>();
            rackToBlocks.put(rack, blklist);
          }
          blklist.add(oneblock);
          if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {
            // Add this host to rackToNodes map
            addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
          }
        }

        // add this block to the node --> block map
        for (int j = 0; j < oneblock.hosts.length; j++) {
          String node = oneblock.hosts[j];
          Set<OneBlockInfo> blklist = nodeToBlocks.get(node);
          if (blklist == null) {
            blklist = new LinkedHashSet<OneBlockInfo>();
            nodeToBlocks.put(node, blklist);
          }
          blklist.add(oneblock);
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    OneBlockInfo[] getBlocks() {
      return blocks;
    }
  }

  static class OneBlockInfo {
    Path onepath;                // name of this file
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long offset, long len,
      String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
        topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i],
            NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }

    public String toString() {
      return onepath + ":" + offset + ":" + length;
    }
  }

  private static void addHostToRack(Map<String, Set<String>> rackToNodes,
    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }



  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  protected List<FileStatus> listStatus0(Configuration conf) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    Path[] dirs = new Path[1];
    dirs[0] = new Path(StringUtils.unEscapeString(conf.get(INPUT_DIR, "")));
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // get tokens for all the required FileSystems..
    //    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
    //      job.getConfiguration());

    // Whether we need to recursive look into the directory structure
    boolean recursive = conf.getBoolean(INPUT_DIR_RECURSIVE, false);

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();

    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i=0; i < dirs.length; ++i) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(conf);
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(),
                    inputFilter);
                } else {
                  result.add(stat);
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }

    return result;
  }


  protected boolean isSplitable(Configuration conf, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(conf).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    long maxSize = 0;
    Configuration conf = job.getConfiguration();

    maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 0);

    // all the files in input set
    List<FileStatus> stats = listStatus(job);
    List<InputSplit> splits = new ArrayList<>();
    if (stats.size() == 0) {
      return splits;
    }

    getMoreSplits(conf, stats, maxSize, 0, 0, splits);

    return splits;

  }

  void createSplits(Map<String, Set<OneBlockInfo>> nodeToBlocks,
    Map<OneBlockInfo, String[]> blockToNodes,
    Map<String, List<OneBlockInfo>> rackToBlocks,
    long totLength,
    long maxSize,
    long minSizeNode,
    long minSizeRack,
    List<InputSplit> splits) throws InterruptedException, IOException {
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    long curSplitSize = 0;

    int totalNodes = nodeToBlocks.size();
    long totalLength = totLength;

    Multiset<String> splitsPerNode = HashMultiset.create();
    Set<String> completedNodes = new HashSet<String>();

    System.out.println("blockToNodes.size()=" + blockToNodes.size());

    while(true) {
      // it is allowed for maxSize to be 0. Disable smoothing load for such cases

      // process all nodes and create splits that are local to a node. Generate
      // one split per node iteration, and walk over nodes multiple times to
      // distribute the splits across nodes.
      for (Iterator<Map.Entry<String, Set<OneBlockInfo>>> iter = nodeToBlocks
        .entrySet().iterator(); iter.hasNext();) {
        Map.Entry<String, Set<OneBlockInfo>> one = iter.next();

        String node = one.getKey();

        // Skip the node if it has previously been marked as completed.
        if (completedNodes.contains(node)) {
          continue;
        }

        Set<OneBlockInfo> blocksInCurrentNode = one.getValue();

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        Iterator<OneBlockInfo> oneBlockIter = blocksInCurrentNode.iterator();
        while (oneBlockIter.hasNext()) {
          OneBlockInfo oneblock = oneBlockIter.next();

          // Remove all blocks which may already have been assigned to other
          // splits.
          if(!blockToNodes.containsKey(oneblock)) {
            oneBlockIter.remove();
            continue;
          }

          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          // if the accumulated split size exceeds the maximum, then
          // create this split.
          if (maxSize != 0 && curSplitSize >= maxSize) {
            // create an input split and add it to the splits array
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            curSplitSize = 0;

            splitsPerNode.add(node);

            // Remove entries from blocksInNode so that we don't walk these
            // again.
            blocksInCurrentNode.removeAll(validBlocks);
            validBlocks.clear();

            // Done creating a single split for this node. Move on to the next
            // node so that splits are distributed across nodes.
            break;
          }

        }
        if (validBlocks.size() != 0) {
          // This implies that the last few blocks (or all in case maxSize=0)
          // were not part of a split. The node is complete.

          // if there were any blocks left over and their combined size is
          // larger than minSplitNode, then combine them into one split.
          // Otherwise add them back to the unprocessed pool. It is likely
          // that they will be combined with other blocks from the
          // same rack later on.
          // This condition also kicks in when max split size is not set. All
          // blocks on a node will be grouped together into a single split.
          if (minSizeNode != 0 && curSplitSize >= minSizeNode
            && splitsPerNode.count(node) == 0) {
            // haven't created any split on this machine. so its ok to add a
            // smaller one for parallelism. Otherwise group it in the rack for
            // balanced size create an input split and add it to the splits
            // array
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            splitsPerNode.add(node);
            // Remove entries from blocksInNode so that we don't walk this again.
            blocksInCurrentNode.removeAll(validBlocks);
            // The node is done. This was the last set of blocks for this node.
          } else {
            // Put the unplaced blocks back into the pool for later rack-allocation.
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
          }
          validBlocks.clear();
          curSplitSize = 0;
          completedNodes.add(node);
        } else { // No in-flight blocks.
          if (blocksInCurrentNode.size() == 0) {
            // Node is done. All blocks were fit into node-local splits.
            completedNodes.add(node);
          } // else Run through the node again.
        }
      }

      // Check if node-local assignments are complete.
      if (completedNodes.size() == totalNodes || totalLength == 0) {
        // All nodes have been walked over and marked as completed or all blocks
        // have been assigned. The rest should be handled via rackLock assignment.
        //        LOG.info("DEBUG: Terminated node allocation with : CompletedNodes: "
        //          + completedNodes.size() + ", size left: " + totalLength);
        break;
      }
    }

    System.out.println("After node splits");
    System.out.println("blockToNodes.size()=" + blockToNodes.size());
    System.out.println("allocated splits size=" + splits.size());
    for (InputSplit split : splits) {
      System.out.println(split.getLength());
    }

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these
    // overflow blocks will be combined into splits.
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    Set<String> racks = new HashSet<String>();

    // Process all racks over and over again until there is no more work to do.
    while (blockToNodes.size() > 0) {

      // Create one split for this rack before moving over to the next rack.
      // Come back to this rack after creating a single split for each of the
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // iterate over all racks
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter =
           rackToBlocks.entrySet().iterator(); iter.hasNext();) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();

        System.out.println("rack info " + one.getKey());

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {

            for (String host: oneblock.hosts) {
              System.out.print(host + " ");
            }
            System.out.println();

            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;

            // if the accumulated split size exceeds the maximum, then
            // create this split.
            if (maxSize != 0 && curSplitSize >= maxSize) {
              // create an input split and add it to the splits array
              addCreatedSplit(splits, getHosts(racks), validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            // if there is a minimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(splits, getHosts(racks), validBlocks);
          } else {
            // There were a few blocks in this rack that
            // remained to be processed. Keep them in 'overflow' block list.
            // These will be combined later.
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    System.out.println("After rack splits");
    System.out.println("blockToNodes.size()=" + blockToNodes.size());
    System.out.println("allocated splits size=" + splits.size());



    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be ok.
      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then
      // create this split.
      if (maxSize != 0 && curSplitSize >= maxSize) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, getHosts(racks), validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(splits, getHosts(racks), validBlocks);
    }
  }

  private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }

  private void addCreatedSplit(List<InputSplit> splitList,
    Collection<String> locations,
    ArrayList<OneBlockInfo> validBlocks) {
    // create an input split
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath;
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }
    // add this split to the list that is returned
    CombineFileSplit thissplit = new CombineFileSplit(fl, offset,
      length, locations.toArray(new String[0]));
    splitList.add(thissplit);
  }


  void createSplitsGreedy(Map<String, Set<OneBlockInfo>> nodeToBlocks,
    Map<OneBlockInfo, String[]> blockToNodes,
    Map<String, List<OneBlockInfo>> rackToBlocks,
    long totLength,
    int num,
    long minSizeNode,
    long minSizeRack,
    List<InputSplit> splits) {

    List<OneBlockInfo> blocks = new ArrayList<>(blockToNodes.keySet());
    Collections.sort(blocks, new Comparator<OneBlockInfo>() {
      @Override
      public int compare(OneBlockInfo o1, OneBlockInfo o2) {
        return -(int) (o1.length - o2.length);
      }
    });

    Int2LongOpenHashMap loads = new Int2LongOpenHashMap();
    Map<Integer, ArrayList<OneBlockInfo>> parts = new HashMap<>();
    Map<Integer, Set<String>> locations = new HashMap<>();

    //    long num = totLength / maxSize;

    for (int i = 0; i < num; i ++) {
      parts.put(i, new ArrayList<OneBlockInfo>());
      locations.put(i, new HashSet<String>());
    }

    for (OneBlockInfo blockInfo : blocks) {

      long min = Long.MAX_VALUE;
      int selectPart = -1;
      for (int s = 0; s < num; s ++) {
        if (loads.get(s) < min) {
          min = loads.get(s);
          selectPart = s;
        }
      }

      loads.addTo(selectPart, blockInfo.length);
      parts.get(selectPart).add(blockInfo);
      for (String host : blockInfo.hosts)
        locations.get(selectPart).add(host);
    }

    for (Map.Entry<Integer, ArrayList<OneBlockInfo>> entry: parts.entrySet()) {
      addCreatedSplit(splits, locations.get(entry.getKey()), entry.getValue());
    }
  }

  public void getMoreSplits(Configuration conf, List<FileStatus> stats,
    long maxSize, long minSizeNode, long minSizeRack,
    List<InputSplit> splits) throws IOException {
    // all blocks for all the files in input set
    OneFileInfo[] files;

    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks =
      new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes =
      new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, Set<OneBlockInfo>> nodeToBlocks =
      new HashMap<String, Set<OneBlockInfo>>();

    files = new OneFileInfo[stats.size()];
    if (stats.size() == 0) {
      return;
    }

    // populate all the blocks for all files
    long totLength = 0;
    int i = 0;
    for (FileStatus stat : stats) {
      files[i] = new OneFileInfo(stat, conf, isSplitable(conf, stat.getPath()),
        rackToBlocks, blockToNodes, nodeToBlocks,
        rackToNodes, 256 * 1024 * 1024);
      totLength += files[i].getLength();
    }

    int partNum = (int) (totLength / maxSize);

    createSplitsGreedy(nodeToBlocks, blockToNodes, rackToBlocks, totLength,
      partNum, minSizeNode, minSizeRack, splits);

  }
}
