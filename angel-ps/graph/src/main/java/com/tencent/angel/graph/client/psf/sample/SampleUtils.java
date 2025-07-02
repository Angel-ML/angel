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

package com.tencent.angel.graph.client.psf.sample;

import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleType;
import com.tencent.angel.graph.data.GraphNode;
import com.tencent.angel.graph.data.MultiGraphNode;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Random;
import scala.Tuple2;
import scala.Tuple3;

public class SampleUtils {

  public static Long2ObjectOpenHashMap<long[]> sampleByCount(ServerRow row, int count,
      long[] nodeIds, long seed) {
    Random r = new Random(seed);
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    for (long nodeId : nodeIds) {
      long[] sampleNeighbors;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = new long[0];
      } else {
        long[] nodeNeighbors = graphNode.getNeighbors();
        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          sampleNeighbors = new long[0];
        } else if (count <= 0 || nodeNeighbors.length <= count) {
          sampleNeighbors = nodeNeighbors;
        } else {
          sampleNeighbors = new long[count];
          // If the neighbor number > count, just copy a range of neighbors to
          // the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos,
                sampleNeighbors, 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, sampleNeighbors, 0,
                nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, sampleNeighbors,
                nodeNeighbors.length - startPos,
                count - (nodeNeighbors.length - startPos));
          }
        }
      }
      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
    }
    return nodeId2SampleNeighbors;
  }

  public static Long2ObjectOpenHashMap<long[]> sampleByName(
          ServerRow row, int count, long[] nodeIds, String name, long seed) {
    Random r = new Random(seed);
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
            new Long2ObjectOpenHashMap<>(nodeIds.length);
    for (long nodeId : nodeIds) {
      long[] sampleNeighbors;
      // Get node neighbor number
      MultiGraphNode graphNode = (MultiGraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null || graphNode.getNeighbors() == null) {
        sampleNeighbors = new long[0];
      } else {
        long[] nodeNeighbors = graphNode.getNeighbors()
                .getOrDefault(name, new long[0]);
        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          sampleNeighbors = new long[0];
        } else if (count <= 0) {
          sampleNeighbors = nodeNeighbors;
        } else {
          sampleNeighbors = new long[count];
          // If the neighbor number > count, just copy a range of neighbors to
          // the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos,
                    sampleNeighbors, 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, sampleNeighbors, 0,
                    nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, sampleNeighbors,
                    nodeNeighbors.length - startPos,
                    count - (nodeNeighbors.length - startPos));
          }
        }
      }
      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
    }
    return nodeId2SampleNeighbors;
  }

  public static Long2ObjectOpenHashMap<long[]> sampleByType(
          ServerRow row, int count, long[] nodeIds, SampleType sampleType, int node_or_edge_type, long seed) {

    Random r = new Random(seed);
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
            new Long2ObjectOpenHashMap<>(nodeIds.length);
    for (long nodeId : nodeIds) {
      long[] sampleNeighbors;
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = new long[0];
      } else {
        Int2ObjectOpenHashMap<long[]> typeNeighbors = graphNode.getTypeNeighbors();
        long[] nodeNeighbors = new long[0];
        if (typeNeighbors != null && typeNeighbors.containsKey(node_or_edge_type)) {
          nodeNeighbors = typeNeighbors.get(node_or_edge_type);
        }
        if (nodeNeighbors.length == 0) {
          sampleNeighbors = new long[0];
        } else if (count <= 0) {
          sampleNeighbors = nodeNeighbors;
        } else {
          int startPos = 0;
          int len = nodeNeighbors.length;
          startPos = r.nextInt(len);
          sampleNeighbors = randomSampling(nodeNeighbors, count, len, startPos);
          if (count > len) {
            LongArrayList sampleNeighborsList = new LongArrayList(sampleNeighbors);
            for (int i = len; i < count; i++) {
              sampleNeighborsList.add(sampleNeighbors[i % len]);
            }
            sampleNeighbors = sampleNeighborsList.toLongArray();
          }
        }
      }
      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
    }
    return nodeId2SampleNeighbors;
  }

  public static Tuple2<Long2ObjectOpenHashMap<long[]>, Long2ObjectOpenHashMap<int[]>> sampleWithType(
      ServerRow row, int count,
      long[] nodeIds, SampleType sampleType, long seed) {

    Random r = new Random(seed);
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleNeighborsType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (long nodeId : nodeIds) {
      long[] sampleNeighbors;
      int[] neighborsTypes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = new long[0];
        neighborsTypes = new int[0];
      } else {
        long[] nodeNeighbors = graphNode.getNeighbors();
        int[] nodeNeighborsTypes;
        if (sampleType == SampleType.NODE) {
          nodeNeighborsTypes = graphNode.getTypes();
        } else {
          nodeNeighborsTypes = graphNode.getEdgeTypes();
        }

        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          sampleNeighbors = new long[0];
          neighborsTypes = new int[0];
        } else if (count <= 0 || nodeNeighbors.length <= count) {
          sampleNeighbors = nodeNeighbors;
          if (nodeNeighborsTypes == null || nodeNeighborsTypes.length == 0) {
            neighborsTypes = new int[0];
          } else {
            neighborsTypes = nodeNeighborsTypes;
          }
        } else {
          sampleNeighbors = new long[count];
          neighborsTypes = new int[count];
          // If the neighbor number > count, just copy a range of neighbors to
          // the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos,
                sampleNeighbors, 0, count);
            System.arraycopy(nodeNeighborsTypes, startPos,
                neighborsTypes, 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, sampleNeighbors, 0,
                nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, sampleNeighbors,
                nodeNeighbors.length - startPos,
                count - (nodeNeighbors.length - startPos));
            //sample types
            System.arraycopy(nodeNeighborsTypes, startPos, neighborsTypes, 0,
                nodeNeighborsTypes.length - startPos);
            System.arraycopy(nodeNeighborsTypes, 0, neighborsTypes,
                nodeNeighborsTypes.length - startPos,
                count - (nodeNeighborsTypes.length - startPos));
          }
        }
      }

      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
      nodeId2SampleNeighborsType.put(nodeId, neighborsTypes);
    }
    return new Tuple2<>(nodeId2SampleNeighbors, nodeId2SampleNeighborsType);
  }

  public static Tuple2<Long2ObjectOpenHashMap<long[]>, Long2ObjectOpenHashMap<int[]>> sampleWithType(
      ServerRow row,
      long[] nodeIds, SampleType sampleType, long[] filterWithNeighKeys, long[] filterWithoutNeighKeys,
      long seed) {
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleNeighborsType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (int i = 0; i < nodeIds.length; i++) {
      ArrayList<Long> sampleNeighbors;
      ArrayList<Integer> neighborsTypes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeIds[i]);
      if (graphNode == null) {
        sampleNeighbors = new ArrayList<>();
        neighborsTypes = new ArrayList<>();
      } else {
        long[] nodeNeighbors = graphNode.getNeighbors();
        int[] nodeNeighborsTypes;
        if (sampleType == SampleType.NODE) {
          nodeNeighborsTypes = graphNode.getTypes();
        } else {
          nodeNeighborsTypes = graphNode.getEdgeTypes();
        }

        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          sampleNeighbors = new ArrayList<>();
          neighborsTypes = new ArrayList<>();
        } else {
          sampleNeighbors = new ArrayList<>();
          neighborsTypes = new ArrayList<>();

          for (int j = 0; j < nodeNeighbors.length; j++) {
            if (filterWithoutNeighKeys != null && nodeNeighbors[j] != filterWithoutNeighKeys[i]) {
              sampleNeighbors.add(nodeNeighbors[j]);
              neighborsTypes.add(nodeNeighborsTypes[j]);
            } else if (filterWithoutNeighKeys == null) {
              if (!sampleNeighbors.contains(nodeNeighbors[j])) {
                sampleNeighbors.add(nodeNeighbors[j]);
                neighborsTypes.add(nodeNeighborsTypes[j]);
              }
            }

            if (filterWithNeighKeys != null) {
              for (int k = 0; k < filterWithNeighKeys.length; k++) {
                if (nodeNeighbors[j] == filterWithNeighKeys[k] && !sampleNeighbors
                    .contains(filterWithNeighKeys[k])) {
                  sampleNeighbors.add(nodeNeighbors[j]);
                  neighborsTypes.add(nodeNeighborsTypes[j]);
                }
              }
            } else {
              if (!sampleNeighbors.contains(nodeNeighbors[j])) {
                sampleNeighbors.add(nodeNeighbors[j]);
                neighborsTypes.add(nodeNeighborsTypes[j]);
              }
            }
          }
        }
      }

      nodeId2SampleNeighbors.put(nodeIds[i], sampleNeighbors.stream().mapToLong(x -> x).toArray());
      nodeId2SampleNeighborsType
          .put(nodeIds[i], neighborsTypes.stream().mapToInt(x -> x).toArray());
    }
    return new Tuple2<>(nodeId2SampleNeighbors, nodeId2SampleNeighborsType);
  }


  public static Tuple3<Long2ObjectOpenHashMap<long[]>, Long2ObjectOpenHashMap<int[]>,
      Long2ObjectOpenHashMap<int[]>> sampleWithBothType(ServerRow row, int count, long[] nodeIds,
      long seed) {

    Random r = new Random(seed);
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleNeighborsType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleEdgeType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (long nodeId : nodeIds) {
      long[] sampleNeighbors;
      int[] neighborsTypes;
      int[] edgeTypes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = new long[0];
        neighborsTypes = new int[0];
        edgeTypes = new int[0];
      } else {
        long[] nodeNeighbors = graphNode.getNeighbors();
        int[] nodeNeighborsTypes = graphNode.getTypes();
        int[] nodeEdgeTypes = graphNode.getEdgeTypes();

        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          sampleNeighbors = new long[0];
          neighborsTypes = new int[0];
          edgeTypes = new int[0];
        } else if (count <= 0 || nodeNeighbors.length <= count) {
          sampleNeighbors = nodeNeighbors;
          if (nodeNeighborsTypes == null || nodeNeighborsTypes.length == 0) {
            neighborsTypes = new int[0];
          } else {
            neighborsTypes = nodeNeighborsTypes;
          }
          if (nodeEdgeTypes == null || nodeEdgeTypes.length == 0) {
            edgeTypes = new int[0];
          } else {
            edgeTypes = nodeEdgeTypes;
          }
        } else {
          sampleNeighbors = new long[count];
          neighborsTypes = new int[count];
          edgeTypes = new int[count];
          // If the neighbor number > count, just copy a range of neighbors to
          // the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos,
                sampleNeighbors, 0, count);
            System.arraycopy(nodeNeighborsTypes, startPos,
                neighborsTypes, 0, count);
            System.arraycopy(nodeEdgeTypes, startPos,
                edgeTypes, 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, sampleNeighbors, 0,
                nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, sampleNeighbors,
                nodeNeighbors.length - startPos,
                count - (nodeNeighbors.length - startPos));
            //sample node types
            System.arraycopy(nodeNeighborsTypes, startPos, neighborsTypes, 0,
                nodeNeighborsTypes.length - startPos);
            System.arraycopy(nodeNeighborsTypes, 0, neighborsTypes,
                nodeNeighborsTypes.length - startPos,
                count - (nodeNeighborsTypes.length - startPos));

            //sample edge types
            System.arraycopy(nodeEdgeTypes, startPos, edgeTypes, 0,
                nodeEdgeTypes.length - startPos);
            System.arraycopy(nodeEdgeTypes, 0, edgeTypes,
                nodeEdgeTypes.length - startPos,
                count - (nodeEdgeTypes.length - startPos));
          }
        }
      }

      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
      nodeId2SampleNeighborsType.put(nodeId, neighborsTypes);
      nodeId2SampleEdgeType.put(nodeId, edgeTypes);
    }
    return new Tuple3<>(nodeId2SampleNeighbors, nodeId2SampleNeighborsType, nodeId2SampleEdgeType);
  }

  public static IntFloatVector[] sampleNodeFeatByCount(ServerRow row, int count, long seed) {
    Random r = new Random(seed);
    IntFloatVector[] feats = new IntFloatVector[count];

    int bound = row.size() - count;
    int skip = bound > 0 ? r.nextInt(bound) : 0;
    ObjectIterator<Entry<IElement>> it = (((ServerLongAnyRow)row).getStorage()).iterator();
    it.skip(skip);
    for (int i = 0; i < count; i++) {
      feats[i] = ((GraphNode) it.next().getValue()).getFeats();
    }

    return feats;
  }

  public static Tuple2<Long2ObjectOpenHashMap<long[]>, Long2ObjectOpenHashMap<IntFloatVector[]>> sampleEdgeFeat(
      ServerRow row, int count,
      long[] nodeIds, long seed) {

    Random r = new Random(seed);
    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<IntFloatVector[]> nodeId2SampleEdgeFeats =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (long nodeId : nodeIds) {
      long[] sampleNeighbors;
      IntFloatVector[] edgeFeats;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = new long[0];
        edgeFeats = new IntFloatVector[0];
      } else {
        long[] nodeNeighbors = graphNode.getNeighbors();
        IntFloatVector[] nodeEdgeFeats = graphNode.getEdgeFeatures();

        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          sampleNeighbors = new long[0];
          edgeFeats = new IntFloatVector[0];
        } else if (count <= 0 || nodeNeighbors.length <= count) {
          sampleNeighbors = nodeNeighbors;
          if (nodeEdgeFeats == null || nodeEdgeFeats.length == 0) {
            edgeFeats = new IntFloatVector[0];
          } else {
            edgeFeats = nodeEdgeFeats;
          }
        } else {
          sampleNeighbors = new long[count];
          edgeFeats = new IntFloatVector[count];
          // If the neighbor number > count, just copy a range of neighbors to
          // the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos,
                sampleNeighbors, 0, count);
            System.arraycopy(nodeEdgeFeats, startPos,
                edgeFeats, 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, sampleNeighbors, 0,
                nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, sampleNeighbors,
                nodeNeighbors.length - startPos,
                count - (nodeNeighbors.length - startPos));
            //sample types
            System.arraycopy(nodeEdgeFeats, startPos, edgeFeats, 0,
                nodeEdgeFeats.length - startPos);
            System.arraycopy(nodeEdgeFeats, 0, edgeFeats,
                nodeEdgeFeats.length - startPos,
                count - (nodeEdgeFeats.length - startPos));
          }
        }
      }

      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
      nodeId2SampleEdgeFeats.put(nodeId, edgeFeats);
    }
    return new Tuple2<>(nodeId2SampleNeighbors, nodeId2SampleEdgeFeats);
  }

  public static long[] randomSampling(long[] nodeNeighbors, int count, int len, int startPos) {
    long[] sampleNeighbors;
    // If the neighbor number > count, just copy a range of neighbors to
    // the result array, the copy position is random
    if (count <= 0 || count >= len) {
      sampleNeighbors = nodeNeighbors;
    } else {
      sampleNeighbors = new long[count];
      if (startPos + count <= len) {
        System.arraycopy(nodeNeighbors, startPos,
                sampleNeighbors, 0, count);
      } else {
        System.arraycopy(nodeNeighbors, startPos, sampleNeighbors, 0,
                nodeNeighbors.length - startPos);
        System.arraycopy(nodeNeighbors, 0, sampleNeighbors,
                nodeNeighbors.length - startPos,
                count - (nodeNeighbors.length - startPos));
      }
    }
    return sampleNeighbors;
  }
}
