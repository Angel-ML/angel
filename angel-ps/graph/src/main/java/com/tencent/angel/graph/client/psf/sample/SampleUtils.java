package com.tencent.angel.graph.client.psf.sample;

import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleMethod;
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleType;
import com.tencent.angel.graph.data.AliasTable;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.GraphNode;
import com.tencent.angel.graph.data.IntNeighbor;
import com.tencent.angel.graph.data.LongNeighbor;
import com.tencent.angel.graph.data.MultiGraphNode;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Random;
import scala.Tuple2;
import scala.Tuple3;

public class SampleUtils {

  public static Neighbor emptyNeighbor(NeighborStorageType storageType) {
    if (storageType == NeighborStorageType.LONGARRAY) {
      return new LongNeighbor(new long[0]);
    } else {
      return new IntNeighbor(new int[0]);
    }
  }

  public static Long2ObjectOpenHashMap<Neighbor> sampleByCount(ServerRow row, int count,
      long[] nodeIds, NeighborStorageType storageType, long seed) {
    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    for (long nodeId : nodeIds) {
      Neighbor sampleNeighbors;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();

        if (nodeNeighbors == null || nodeNeighbors.getNeighborsNum() == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
        } else {
          sampleNeighbors = nodeNeighbors.sampleNeighbors(count, false, seed);
        }
      }
      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
    }
    return nodeId2SampleNeighbors;
  }

  public static Long2ObjectOpenHashMap<Neighbor> sampleByName(ServerRow row, int count,
      long[] nodeIds, String name, SampleMethod method, NeighborStorageType storageType,
      long seed) {

    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    for (long nodeId : nodeIds) {
      Neighbor sampleNeighbors;
      // Get node neighbor number
      MultiGraphNode graphNode = (MultiGraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null || graphNode.getNeighbors() == null) {
        sampleNeighbors = emptyNeighbor(storageType);
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors()
            .getOrDefault(name, emptyNeighbor(storageType));
        if (nodeNeighbors == null || nodeNeighbors.getNeighborsNum() == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
        } else if (count <= 0) {
          sampleNeighbors = nodeNeighbors;
        } else {
          switch (method) {
            case SIMPLEREJECT:
              sampleNeighbors = rejectSampling(nodeNeighbors, graphNode.getWeights(name), count,
                  seed);
              break;
            case ALIASTABLE:
              sampleNeighbors = aliasTableSampling(nodeNeighbors, graphNode.getAliasTables(name),
                  count, seed);
              break;
            default:
              sampleNeighbors = nodeNeighbors.sampleNeighbors(count, false, seed);
          }
        }
      }
      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
    }
    return nodeId2SampleNeighbors;
  }

  public static Long2ObjectOpenHashMap<Neighbor> sampleByWeight(ServerRow row, int count,
      long[] nodeIds, SampleMethod method, NeighborStorageType storageType, long seed) {

    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    for (long nodeId : nodeIds) {
      Neighbor sampleNeighbors;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();
        if (nodeNeighbors == null || nodeNeighbors.getNeighborsNum() == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
        } else if (count <= 0) {
          sampleNeighbors = nodeNeighbors;
        } else {
          switch (method) {
            case SIMPLEREJECT:
              sampleNeighbors = rejectSampling(nodeNeighbors, graphNode.getWeights(), count, seed);
              break;
            case ALIASTABLE:
              sampleNeighbors = aliasTableSampling(nodeNeighbors, graphNode.getAliasTable(), count,
                  seed);
              break;
            default:
              sampleNeighbors = nodeNeighbors.sampleNeighbors(count, false, seed);
          }
        }
      }
      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
    }
    return nodeId2SampleNeighbors;
  }

  public static Long2ObjectOpenHashMap<Neighbor> sampleByType(ServerRow row, int count,
      long[] nodeIds, SampleType sampleType, int node_or_edge_type, SampleMethod method,
      NeighborStorageType storageType, long seed) {

    Random r = new Random(seed);
    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
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
      nodeId2SampleNeighbors.put(nodeId, new LongNeighbor(sampleNeighbors));
    }
    return nodeId2SampleNeighbors;
  }

  public static Tuple2<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<int[]>> sampleWithType(
      ServerRow row, int count,
      long[] nodeIds, SampleType sampleType, SampleMethod method, NeighborStorageType storageType,
      long seed) {

    Random r = new Random();
    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleNeighborsType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (long nodeId : nodeIds) {
      Neighbor sampleNeighbors;
      int[] neighborsTypes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
        neighborsTypes = new int[0];
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();
        int[] nodeNeighborsTypes;
        if (sampleType == SampleType.NODE) {
          nodeNeighborsTypes = graphNode.getTypes();
        } else {
          nodeNeighborsTypes = graphNode.getEdgeTypes();
        }

        if (nodeNeighbors == null || nodeNeighbors.getNeighborsNum() == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
          neighborsTypes = new int[0];
        } else if (count <= 0) {
          sampleNeighbors = nodeNeighbors;
          if (nodeNeighborsTypes == null || nodeNeighborsTypes.length == 0) {
            neighborsTypes = new int[0];
          } else {
            neighborsTypes = nodeNeighborsTypes;
          }
        } else {
          int len = nodeNeighbors.getNeighborsNum();
          Tuple2<Neighbor, int[]> sampleRes;

          switch (method) {
            case SIMPLEREJECT:
              sampleRes = rejectSampling(nodeNeighbors, nodeNeighborsTypes, graphNode.getWeights(),
                  count, seed);
              sampleNeighbors = sampleRes._1;
              neighborsTypes = sampleRes._2;
              break;

            case ALIASTABLE:
              sampleRes = aliasTableSampling(nodeNeighbors, nodeNeighborsTypes,
                  graphNode.getAliasTable(), count, seed);
              sampleNeighbors = sampleRes._1;
              neighborsTypes = sampleRes._2;
              break;

            default:
              int startPos = r.nextInt(len);
              sampleNeighbors = nodeNeighbors.sampleNeighbors(count, startPos);
              neighborsTypes = randomSampling(nodeNeighborsTypes, count, len, startPos);
          }
        }
      }

      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
      nodeId2SampleNeighborsType.put(nodeId, neighborsTypes);
    }
    return new Tuple2<>(nodeId2SampleNeighbors, nodeId2SampleNeighborsType);
  }

  public static Tuple2<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<int[]>> sampleWithType(
      ServerRow row, long[] nodeIds, SampleType sampleType, int count,
      long[] filterWithoutNeighKeys,
      long[] filterWithNeighKeys, NeighborStorageType storageType, long seed) {
    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleNeighborsType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (int i = 0; i < nodeIds.length; i++) {
      Neighbor sampleNeighbors;
      int[] neighborsTypes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeIds[i]);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
        neighborsTypes = new int[0];
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();
        int len = 0;
        if (nodeNeighbors != null) {
          len = nodeNeighbors.getNeighborsNum();
        }

        int[] nodeNeighborsTypes;
        if (sampleType == SampleType.NODE) {
          nodeNeighborsTypes = graphNode.getTypes();
        } else {
          nodeNeighborsTypes = graphNode.getEdgeTypes();
        }

        // node neighbors is null or the num of neighbors is 0
        if (nodeNeighbors == null || len == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
          neighborsTypes = new int[0];
        } else { // sample neighbors

          Tuple2<Neighbor, int[]> result = randomSampleWithType(nodeNeighbors, nodeNeighborsTypes,
              filterWithoutNeighKeys != null,
              filterWithoutNeighKeys == null ? -1L : filterWithoutNeighKeys[i], filterWithNeighKeys,
              count, seed);
          sampleNeighbors = result._1;
          neighborsTypes = result._2;
        }
      }

      nodeId2SampleNeighbors.put(nodeIds[i], sampleNeighbors);
      nodeId2SampleNeighborsType.put(nodeIds[i], neighborsTypes);
    }
    return new Tuple2<>(nodeId2SampleNeighbors, nodeId2SampleNeighborsType);
  }

  public static Tuple3<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<int[]>,
      Long2ObjectOpenHashMap<int[]>> sampleWithBothType(ServerRow row, int count, long[] nodeIds,
      SampleMethod method, NeighborStorageType storageType, long seed) {
    Random r = new Random();
    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleNeighborsType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<int[]> nodeId2SampleEdgeType =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (long nodeId : nodeIds) {
      Neighbor sampleNeighbors;
      int[] neighborsTypes;
      int[] edgeTypes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
        neighborsTypes = new int[0];
        edgeTypes = new int[0];
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();
        int[] nodeNeighborsTypes = graphNode.getTypes();
        int[] nodeEdgeTypes = graphNode.getEdgeTypes();

        if (nodeNeighbors == null || nodeNeighbors.getNeighborsNum() == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
          neighborsTypes = new int[0];
          edgeTypes = new int[0];
        } else if (count <= 0) {
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
          int startPos;
          int len = nodeNeighbors.getNeighborsNum();
          Tuple3<Neighbor, int[], int[]> result;

          switch (method) {
            case SIMPLEREJECT:
              result = rejectSampling(nodeNeighbors, nodeNeighborsTypes, nodeEdgeTypes, count,
                  graphNode.getWeights(), seed);
              sampleNeighbors = result._1();
              neighborsTypes = result._2();
              edgeTypes = result._3();
              break;

            case ALIASTABLE:
              result = aliasTableSampling(nodeNeighbors, nodeNeighborsTypes, nodeEdgeTypes,
                  graphNode.getAliasTable(), count, seed);
              sampleNeighbors = result._1();
              neighborsTypes = result._2();
              edgeTypes = result._3();
              break;

            default:
              startPos = r.nextInt(len);
              sampleNeighbors = nodeNeighbors.sampleNeighbors(count, startPos);
              neighborsTypes = randomSampling(nodeNeighborsTypes, count, len, startPos);
              edgeTypes = randomSampling(nodeEdgeTypes, count, len, startPos);

          }
        }
      }

      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
      nodeId2SampleNeighborsType.put(nodeId, neighborsTypes);
      nodeId2SampleEdgeType.put(nodeId, edgeTypes);
    }
    return new Tuple3<>(nodeId2SampleNeighbors, nodeId2SampleNeighborsType, nodeId2SampleEdgeType);
  }

  public static Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> sampleWithTime(
      ServerRow row, long[] nodeIds, int[] timestamps, SampleMethod method, int count,
      NeighborStorageType storageType, long seed) {
    Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeId2SampleNeighbors =
        new Object2ObjectOpenHashMap<>(nodeIds.length);
    assert nodeIds.length == timestamps.length;

    for (int i = 0; i < nodeIds.length; i++) {
      Neighbor sampleNeighbors;
      int[] neighborsTimes;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeIds[i]);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
        neighborsTimes = new int[0];
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();
        int[] edgeTimestamps = graphNode.getEdgeTimestamps();
        int len = 0;
        if (nodeNeighbors != null) {
          len = nodeNeighbors.getNeighborsNum();
        }

        // node neighbors is null or the num of neighbors is 0
        if (nodeNeighbors == null || len == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
          neighborsTimes = new int[0];
        } else { // sample neighbors
          switch (method) {
            case RANDOM: {
              Tuple2<Neighbor, int[]> result = randomSampleWithTime(nodeIds[i], nodeNeighbors,
                  edgeTimestamps,
                  timestamps[i], count, seed);
              sampleNeighbors = result._1;
              neighborsTimes = result._2;
            }
            break;

            default:
              throw new UnsupportedOperationException(
                  "Not support sample method " + method + " now");
          }
        }
      }

      nodeId2SampleNeighbors.put(new Tuple2<>(nodeIds[i], timestamps[i]),
          new Tuple2<>(sampleNeighbors, neighborsTimes));
    }
    return nodeId2SampleNeighbors;
  }

  public static ByteArrayElement[] sampleNodeFeatByCount(ServerRow row, int count, long seed) {
    Random r = new Random(seed);
    ByteArrayElement[] feats = new ByteArrayElement[count];

    int bound = row.size() - count;
    int skip = bound > 0 ? r.nextInt(bound) : 0;
    ObjectIterator<Entry<IElement>> it = (((ServerLongAnyRow) row).getStorage()).iterator();
    it.skip(skip);
    for (int i = 0; i < count; i++) {
      feats[i] = ((GraphNode) it.next().getValue()).getFeats();
    }

    return feats;
  }

  public static Tuple2<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<Feature[]>> sampleEdgeFeat(
      ServerRow row, int count, long[] nodeIds, SampleMethod method,
      NeighborStorageType storageType, long seed) {

    Random r = new Random();
    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors =
        new Long2ObjectOpenHashMap<>(nodeIds.length);
    Long2ObjectOpenHashMap<Feature[]> nodeId2SampleEdgeFeats =
        new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (long nodeId : nodeIds) {
      Neighbor sampleNeighbors;
      Feature[] edgeFeats;
      // Get node neighbor number
      GraphNode graphNode = (GraphNode) ((ServerLongAnyRow) row).get(nodeId);
      if (graphNode == null) {
        sampleNeighbors = emptyNeighbor(storageType);
        edgeFeats = new Feature[0];
      } else {
        Neighbor nodeNeighbors = graphNode.getNeighbors();
        Feature[] nodeEdgeFeats = graphNode.getEdgeFeatures();

        if (nodeNeighbors == null || nodeNeighbors.getNeighborsNum() == 0) {
          sampleNeighbors = emptyNeighbor(storageType);
          edgeFeats = new Feature[0];
        } else if (count <= 0) {
          sampleNeighbors = nodeNeighbors;
          if (nodeEdgeFeats == null || nodeEdgeFeats.length == 0) {
            edgeFeats = new Feature[0];
          } else {
            edgeFeats = nodeEdgeFeats;
          }
        } else {
          int len = nodeNeighbors.getNeighborsNum();
          Tuple2<Neighbor, Feature[]> result;

          switch (method) {
            case SIMPLEREJECT:
              result = rejectSampling(nodeNeighbors, nodeEdgeFeats, count, graphNode.getWeights(),
                  seed);
              sampleNeighbors = result._1;
              edgeFeats = result._2;
              break;

            case ALIASTABLE:
              result = aliasTableSampling(nodeNeighbors, nodeEdgeFeats, graphNode.getAliasTable(),
                  count, seed);
              sampleNeighbors = result._1;
              edgeFeats = result._2;
              break;

            default:
              int startPos = r.nextInt(len);
              sampleNeighbors = nodeNeighbors.sampleNeighbors(count, startPos);
              edgeFeats = randomSampling(nodeEdgeFeats, count, len, startPos);
          }
        }
      }

      nodeId2SampleNeighbors.put(nodeId, sampleNeighbors);
      nodeId2SampleEdgeFeats.put(nodeId, edgeFeats);
    }
    return new Tuple2<>(nodeId2SampleNeighbors, nodeId2SampleEdgeFeats);
  }

  public static long[] randomSampling(long[] nodeNeighbors, int count) {
    Random r = new Random();
    int startPos = r.nextInt(nodeNeighbors.length);
    long[] sampleNeighbors;
    // If the neighbor number > count, just copy a range of neighbors to
    // the result array, the copy position is random
    if (count <= 0 || count >= nodeNeighbors.length) {
      sampleNeighbors = nodeNeighbors;
    } else {
      sampleNeighbors = new long[count];
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
    return sampleNeighbors;
  }

  public static Neighbor rejectSampling(Neighbor nodeNeighbors, float[] weights, int count,
      long seed) {
    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      return new LongNeighbor(
          rejectSampling(((LongNeighbor) nodeNeighbors).getNodeIds(), weights, count, seed));
    } else {
      return new IntNeighbor(
          rejectSampling(((IntNeighbor) nodeNeighbors).getNodeIds(), weights, count, seed));
    }
  }

  public static Tuple2<Neighbor, int[]> rejectSampling(Neighbor nodeNeighbors, int[] neighborsTypes,
      float[] weights, int count, long seed) {
    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      return rejectSampling(((LongNeighbor) nodeNeighbors).getNodeIds(), neighborsTypes, weights,
          count, seed);
    } else {
      return rejectSampling(((IntNeighbor) nodeNeighbors).getNodeIds(), neighborsTypes, weights,
          count, seed);
    }
  }

  public static long[] rejectSampling(long[] nodeNeighbors, float[] weights, int count, long seed) {
    Random r = new Random(seed);
    LongArrayList sampleNeighborsList = new LongArrayList();
    int startPos = 0;
    while (sampleNeighborsList.size() < count) {
      boolean isAccept = false;
      while (!isAccept) {
        startPos = r.nextInt(nodeNeighbors.length);
        if (weights[startPos] > r.nextFloat()) {
          isAccept = true;
        }
      }
      sampleNeighborsList.add(nodeNeighbors[startPos]);
    }

    return sampleNeighborsList.toLongArray();
  }

  public static int[] rejectSampling(int[] nodeNeighbors, float[] weights, int count, long seed) {
    Random r = new Random(seed);
    IntArrayList sampleNeighborsList = new IntArrayList();
    int startPos = 0;
    while (sampleNeighborsList.size() < count) {
      boolean isAccept = false;
      while (!isAccept) {
        startPos = r.nextInt(nodeNeighbors.length);
        if (weights[startPos] > r.nextFloat()) {
          isAccept = true;
        }
      }
      sampleNeighborsList.add(nodeNeighbors[startPos]);
    }

    return sampleNeighborsList.toIntArray();
  }

  public static Tuple2<Neighbor, int[]> rejectSampling(long[] nodeNeighbors, int[] neighborsTypes,
      float[] weights, int count, long seed) {
    Random r = new Random(seed);
    LongArrayList sampleNeighborsList = new LongArrayList();
    IntArrayList sampleNeighborsTypesList = new IntArrayList();
    int startPos = 0;
    while (sampleNeighborsList.size() < count) {
      boolean isAccept = false;
      while (!isAccept) {
        startPos = r.nextInt(nodeNeighbors.length);
        if (weights[startPos] > r.nextFloat()) {
          isAccept = true;
        }
      }
      sampleNeighborsList.add(nodeNeighbors[startPos]);
      sampleNeighborsTypesList.add(neighborsTypes[startPos]);
    }

    return new Tuple2<>(new LongNeighbor(sampleNeighborsList.toLongArray()),
        sampleNeighborsTypesList.toIntArray());
  }

  public static Tuple2<Neighbor, int[]> rejectSampling(int[] nodeNeighbors, int[] neighborsTypes,
      float[] weights, int count, long seed) {
    Random r = new Random(seed);
    IntArrayList sampleNeighborsList = new IntArrayList();
    IntArrayList sampleNeighborsTypesList = new IntArrayList();
    int startPos = 0;
    while (sampleNeighborsList.size() < count) {
      boolean isAccept = false;
      while (!isAccept) {
        startPos = r.nextInt(nodeNeighbors.length);
        if (weights[startPos] > r.nextFloat()) {
          isAccept = true;
        }
      }
      sampleNeighborsList.add(nodeNeighbors[startPos]);
      sampleNeighborsTypesList.add(neighborsTypes[startPos]);
    }

    return new Tuple2<>(new IntNeighbor(sampleNeighborsList.toIntArray()),
        sampleNeighborsTypesList.toIntArray());
  }

  public static Neighbor aliasTableSampling(Neighbor nodeNeighbors, AliasTable aliasTable,
      int count, long seed) {
    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      return new LongNeighbor(
          aliasTableSampling(((LongNeighbor) nodeNeighbors).getNodeIds(), aliasTable, count, seed));
    } else {
      return new IntNeighbor(
          aliasTableSampling(((IntNeighbor) nodeNeighbors).getNodeIds(), aliasTable, count, seed));
    }
  }

  public static Tuple2<Neighbor, int[]> aliasTableSampling(Neighbor nodeNeighbors,
      int[] neighborsTypes, AliasTable aliasTable, int count, long seed) {
    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      return aliasTableSampling(((LongNeighbor) nodeNeighbors).getNodeIds(), neighborsTypes,
          aliasTable, count, seed);
    } else {
      return aliasTableSampling(((IntNeighbor) nodeNeighbors).getNodeIds(), neighborsTypes,
          aliasTable, count, seed);
    }
  }

  public static long[] aliasTableSampling(long[] nodeNeighbors, AliasTable aliasTable, int count,
      long seed) {
    LongArrayList sampleNeighborsList = new LongArrayList();
    while (sampleNeighborsList.size() < count) {
      int selected = aliasTableSampling(nodeNeighbors.length, aliasTable, seed);
      sampleNeighborsList.add(nodeNeighbors[selected]);
    }
    return sampleNeighborsList.toLongArray();
  }

  public static int[] aliasTableSampling(int[] nodeNeighbors, AliasTable aliasTable, int count,
      long seed) {
    IntArrayList sampleNeighborsList = new IntArrayList();
    while (sampleNeighborsList.size() < count) {
      int selected = aliasTableSampling(nodeNeighbors.length, aliasTable, seed);
      sampleNeighborsList.add(nodeNeighbors[selected]);
    }
    return sampleNeighborsList.toIntArray();
  }

  public static Tuple2<Neighbor, int[]> aliasTableSampling(long[] nodeNeighbors,
      int[] neighborsTypes, AliasTable aliasTable, int count, long seed) {
    LongArrayList sampleNeighborsList = new LongArrayList();
    IntArrayList sampleNeighborsTypesList = new IntArrayList();
    while (sampleNeighborsList.size() < count) {
      int selected = aliasTableSampling(nodeNeighbors.length, aliasTable, seed);
      sampleNeighborsList.add(nodeNeighbors[selected]);
      sampleNeighborsTypesList.add(neighborsTypes[selected]);
    }
    return new Tuple2<>(new LongNeighbor(sampleNeighborsList.toLongArray()),
        sampleNeighborsTypesList.toIntArray());
  }

  public static Tuple2<Neighbor, int[]> aliasTableSampling(int[] nodeNeighbors,
      int[] neighborsTypes, AliasTable aliasTable, int count, long seed) {
    IntArrayList sampleNeighborsList = new IntArrayList();
    IntArrayList sampleNeighborsTypesList = new IntArrayList();
    while (sampleNeighborsList.size() < count) {
      int selected = aliasTableSampling(nodeNeighbors.length, aliasTable, seed);
      sampleNeighborsList.add(nodeNeighbors[selected]);
      sampleNeighborsTypesList.add(neighborsTypes[selected]);
    }
    return new Tuple2<>(new IntNeighbor(sampleNeighborsList.toIntArray()),
        sampleNeighborsTypesList.toIntArray());
  }

  public static int aliasTableSampling(int len, AliasTable aliasTable, long seed) {
    float[] accept = aliasTable.getAccept();
    int[] alias = aliasTable.getAlias();
    Random random = new Random(seed);
    int selected = -1;
    if (len > 0 && accept.length > 0 && alias.length > 0) {
      if (len == 1) {
        selected = 0;
      } else {
        int index = random.nextInt(len);
        float ac = random.nextFloat();
        if (ac < accept[index]) {
          selected = index;
        } else {
          selected = alias[index];
        }
      }
    }
    return selected;
  }

  public static Tuple3<Neighbor, int[], int[]> aliasTableSampling(Neighbor nodeNeighbors,
      int[] neighborsTypes, int[] edgeTypes, AliasTable aliasTable, int count, long seed) {
    int len = nodeNeighbors.getNeighborsNum();
    IntArrayList sampleNeighborsTypesList = new IntArrayList();
    IntArrayList sampleedgeTypesList = new IntArrayList();
    int startPos;
    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      LongArrayList sampleNeighborsList = new LongArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = aliasTableSampling(len, aliasTable, seed);
        sampleNeighborsList.add(((LongNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleNeighborsTypesList.add(neighborsTypes[startPos]);
        sampleedgeTypesList.add(edgeTypes[startPos]);
      }
      return new Tuple3<>(new LongNeighbor(sampleNeighborsList.toLongArray()),
          sampleNeighborsTypesList.toIntArray(), sampleedgeTypesList.toIntArray());
    } else {
      IntArrayList sampleNeighborsList = new IntArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = aliasTableSampling(len, aliasTable, seed);
        sampleNeighborsList.add(((IntNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleNeighborsTypesList.add(neighborsTypes[startPos]);
        sampleedgeTypesList.add(edgeTypes[startPos]);
      }
      return new Tuple3<>(new IntNeighbor(sampleNeighborsList.toIntArray()),
          sampleNeighborsTypesList.toIntArray(), sampleedgeTypesList.toIntArray());
    }
  }

  public static Tuple2<Neighbor, Feature[]> aliasTableSampling(Neighbor nodeNeighbors,
      Feature[] nodeFeats, AliasTable aliasTable, int count, long seed) {
    int len = nodeNeighbors.getNeighborsNum();
    Feature[] sampleFeatures = new Feature[count];
    int startPos;
    int i = 0;

    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      LongArrayList sampleNeighborsList = new LongArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = aliasTableSampling(len, aliasTable, seed);
        sampleNeighborsList.add(((LongNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleFeatures[i] = nodeFeats[startPos];
        i++;

      }
      return new Tuple2<>(new LongNeighbor(sampleNeighborsList.toLongArray()), sampleFeatures);
    } else {
      IntArrayList sampleNeighborsList = new IntArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = aliasTableSampling(len, aliasTable, seed);
        sampleNeighborsList.add(((IntNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleFeatures[i] = nodeFeats[startPos];
        i++;
      }
      return new Tuple2<>(new IntNeighbor(sampleNeighborsList.toIntArray()), sampleFeatures);
    }
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

  public static int[] randomSampling(int[] types, int count, int len, int startPos) {
    int[] sampleTypes;
    // If the neighbor number > count, just copy a range of neighbors to
    // the result array, the copy position is random
    if (count <= 0 || count >= len) {
      if (types == null || types.length == 0) {
        sampleTypes = new int[0];
      } else {
        sampleTypes = types;
      }
    } else {
      sampleTypes = new int[count];
      if (startPos + count <= len) {
        System.arraycopy(types, startPos,
            sampleTypes, 0, count);
      } else {
        System.arraycopy(types, startPos, sampleTypes, 0,
            types.length - startPos);
        System.arraycopy(types, 0, sampleTypes,
            types.length - startPos,
            count - (types.length - startPos));
      }
    }
    return sampleTypes;
  }

  public static Feature[] randomSampling(Feature[] features, int count, int len, int startPos) {
    Feature[] sampleFeatures;
    // If the neighbor number > count, just copy a range of neighbors to
    // the result array, the copy position is random
    if (count <= 0 || count >= len) {
      if (features == null || features.length == 0) {
        sampleFeatures = new Feature[0];
      } else {
        sampleFeatures = features;
      }
    } else {
      sampleFeatures = new Feature[count];
      if (startPos + count <= len) {
        System.arraycopy(features, startPos,
            sampleFeatures, 0, count);
      } else {
        System.arraycopy(features, startPos, sampleFeatures, 0,
            features.length - startPos);
        System.arraycopy(features, 0, sampleFeatures,
            features.length - startPos,
            count - (features.length - startPos));
      }
    }
    return sampleFeatures;
  }

  public static int rejectSampling(int len, float[] weights, long seed) {
    Random r = new Random(seed);
    int startPos = 0;
    boolean isAccept = false;
    int k = 10;
    while (!isAccept && k > 0) {
      startPos = r.nextInt(len);
      if (weights[startPos] > r.nextFloat()) {
        isAccept = true;
      }
      k--;
    }

    return startPos;
  }

  public static Tuple3<Neighbor, int[], int[]> rejectSampling(Neighbor nodeNeighbors,
      int[] neighborsTypes, int[] edgeTypes, int count, float[] weights, long seed) {
    int len = nodeNeighbors.getNeighborsNum();
    IntArrayList sampleNeighborsTypesList = new IntArrayList();
    IntArrayList sampleedgeTypesList = new IntArrayList();
    int startPos;
    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      LongArrayList sampleNeighborsList = new LongArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = rejectSampling(len, weights, seed);
        sampleNeighborsList.add(((LongNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleNeighborsTypesList.add(neighborsTypes[startPos]);
        sampleedgeTypesList.add(edgeTypes[startPos]);
      }
      return new Tuple3<>(new LongNeighbor(sampleNeighborsList.toLongArray()),
          sampleNeighborsTypesList.toIntArray(), sampleedgeTypesList.toIntArray());
    } else {
      IntArrayList sampleNeighborsList = new IntArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = rejectSampling(len, weights, seed);
        sampleNeighborsList.add(((IntNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleNeighborsTypesList.add(neighborsTypes[startPos]);
        sampleedgeTypesList.add(edgeTypes[startPos]);
      }
      return new Tuple3<>(new IntNeighbor(sampleNeighborsList.toIntArray()),
          sampleNeighborsTypesList.toIntArray(), sampleedgeTypesList.toIntArray());
    }
  }

  public static Tuple2<Neighbor, Feature[]> rejectSampling(Neighbor nodeNeighbors,
      Feature[] nodeFeats, int count, float[] weights, long seed) {
    int len = nodeNeighbors.getNeighborsNum();
    Feature[] sampleFeatures = new Feature[count];
    int startPos;
    int i = 0;

    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      LongArrayList sampleNeighborsList = new LongArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = rejectSampling(len, weights, seed);
        sampleNeighborsList.add(((LongNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleFeatures[i] = nodeFeats[startPos];
        i++;
      }
      return new Tuple2<>(new LongNeighbor(sampleNeighborsList.toLongArray()), sampleFeatures);
    } else {
      IntArrayList sampleNeighborsList = new IntArrayList();
      while (sampleNeighborsList.size() < count) {
        startPos = rejectSampling(len, weights, seed);
        sampleNeighborsList.add(((IntNeighbor) nodeNeighbors).getNodeIds()[startPos]);
        sampleFeatures[i] = nodeFeats[startPos];
        i++;
      }
      return new Tuple2<>(new IntNeighbor(sampleNeighborsList.toIntArray()), sampleFeatures);
    }
  }

  public static Tuple2<Neighbor, int[]> randomSampleWithType(Neighbor nodeNeighbors,
      int[] nodeNeighborsTypes, boolean filterWithout, long filterWithoutNeighKey,
      long[] filterWithNeighKeys, int count, long seed) {
    int[] neighborsTypes;
    int len = nodeNeighbors.getNeighborsNum();
    int startPos = 0;
    Random r = new Random(seed);

    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      long[] neighs = ((LongNeighbor) nodeNeighbors).getNodeIds();
      long[] sampleNeighbors;

      if (neighs == null) {
        sampleNeighbors = new long[0];
        neighborsTypes = new int[0];
      } else {
        // must not contains some key
        int withoutIdx = -1;
        if (filterWithout) {
          withoutIdx = LongArrays.binarySearch(neighs, filterWithoutNeighKey);
        }

        if (count <= 0 || count >= len) {
          if (withoutIdx >= 0) {
            sampleNeighbors = new long[len - 1];
            neighborsTypes = new int[len - 1];

            System.arraycopy(neighs, startPos, sampleNeighbors, 0,
                withoutIdx - startPos);
            System.arraycopy(neighs, withoutIdx + 1, sampleNeighbors,
                withoutIdx - startPos,
                len - (withoutIdx - startPos + 1));

            System.arraycopy(nodeNeighborsTypes, startPos, neighborsTypes, 0,
                withoutIdx - startPos);
            System.arraycopy(nodeNeighborsTypes, withoutIdx + 1, neighborsTypes,
                withoutIdx - startPos,
                len - (withoutIdx - startPos + 1));
          } else {
            sampleNeighbors = neighs;
            neighborsTypes = nodeNeighborsTypes;
          }
        } else {
          IntOpenHashSet idxs = new IntOpenHashSet();
          // must contains some keys
          if (filterWithNeighKeys != null) {
            for (int k = 0; k < filterWithNeighKeys.length; k++) {
              int idx = LongArrays.binarySearch(neighs, filterWithNeighKeys[k]);
              if (idx > 0 && idx != withoutIdx) {
                idxs.add(idx);
              }
            }
          }

          int num = 0;
          while (num < count) {
            startPos = r.nextInt(len);
            while (startPos == withoutIdx) {
              startPos = r.nextInt(len);
            }
            idxs.add(startPos);

            num += 1;
          }

          LongArrayList sampleNeighborsList = new LongArrayList();
          IntArrayList sampleNeighborsTypesList = new IntArrayList();

          for (int id : idxs) {
            sampleNeighborsList.add(neighs[id]);
            sampleNeighborsTypesList.add(nodeNeighborsTypes[id]);
          }

          sampleNeighbors = sampleNeighborsList.toLongArray();
          neighborsTypes = sampleNeighborsTypesList.toIntArray();
        }
      }
      return new Tuple2<>(new LongNeighbor(sampleNeighbors), neighborsTypes);
    } else {
      int[] neighs = ((IntNeighbor) nodeNeighbors).getNodeIds();
      int[] sampleNeighbors;

      if (neighs == null) {
        sampleNeighbors = new int[0];
        neighborsTypes = new int[0];
      } else {
        // must not contains some key
        int withoutIdx = -1;
        if (filterWithout) {
          withoutIdx = IntArrays.binarySearch(neighs, (int) filterWithoutNeighKey);
        }

        if (count <= 0 || count >= len) {
          if (withoutIdx >= 0) {
            sampleNeighbors = new int[len - 1];
            neighborsTypes = new int[len - 1];

            System.arraycopy(neighs, startPos, sampleNeighbors, 0,
                withoutIdx - startPos);
            System.arraycopy(neighs, withoutIdx + 1, sampleNeighbors,
                withoutIdx - startPos,
                len - (withoutIdx - startPos + 1));

            System.arraycopy(nodeNeighborsTypes, startPos, neighborsTypes, 0,
                withoutIdx - startPos);
            System.arraycopy(nodeNeighborsTypes, withoutIdx + 1, neighborsTypes,
                withoutIdx - startPos,
                len - (withoutIdx - startPos + 1));
          } else {
            sampleNeighbors = neighs;
            neighborsTypes = nodeNeighborsTypes;
          }
        } else {
          IntOpenHashSet idxs = new IntOpenHashSet();
          // must contains some keys
          if (filterWithNeighKeys != null) {
            for (int k = 0; k < filterWithNeighKeys.length; k++) {
              int idx = IntArrays.binarySearch(neighs, (int) filterWithNeighKeys[k]);
              if (idx > 0 && idx != withoutIdx) {
                idxs.add(idx);
              }
            }
          }

          int num = 0;
          while (num < count) {
            startPos = r.nextInt(len);
            while (startPos == withoutIdx) {
              startPos = r.nextInt(len);
            }
            idxs.add(startPos);

            num += 1;
          }

          IntArrayList sampleNeighborsList = new IntArrayList();
          IntArrayList sampleNeighborsTypesList = new IntArrayList();

          for (int id : idxs) {
            sampleNeighborsList.add(neighs[id]);
            sampleNeighborsTypesList.add(nodeNeighborsTypes[id]);
          }

          sampleNeighbors = sampleNeighborsList.toIntArray();
          neighborsTypes = sampleNeighborsTypesList.toIntArray();
        }
      }
      return new Tuple2<>(new IntNeighbor(sampleNeighbors), neighborsTypes);
    }
  }

  public static Tuple2<Neighbor, int[]> randomSampleWithTime(long graphNode, Neighbor nodeNeighbors,
      int[] timestamps,
      int filterTimestamp, int count, long seed) {
    int[] neighborsTimes;
    int len = nodeNeighbors.getNeighborsNum();
    Random r = new Random(seed);

    if (nodeNeighbors.getStorageType() == NeighborStorageType.LONGARRAY) {
      long[] neighs = ((LongNeighbor) nodeNeighbors).getNodeIds();

      long[] sampleNeighbors;

      if (neighs == null) {
        sampleNeighbors = new long[0];
        neighborsTimes = new int[0];
      } else {
        assert (count >= 1);
        sampleNeighbors = new long[count];
        neighborsTimes = new int[count];
        // search the index of filterTimestamp
        int idx = IntArrays.binarySearch(timestamps, filterTimestamp);
        if (idx < 0) {
          idx = Math.abs(idx) - 2;
        }

        if (count >= len) {
          if (idx >= 0) {
            for (int i = 0; i < count; i++) {
              int pos = r.nextInt(idx + 1);
              sampleNeighbors[i] = neighs[pos];
              neighborsTimes[i] = timestamps[pos];
            }
          } else { // idx < 0
            System.out.println("time idx < 0 when count >= len !");
            sampleNeighbors = new long[0];
            neighborsTimes = new int[0];
          }
        } else { // count < len
          if (idx >= 0 && idx < count) {
            for (int i = 0; i < count; i++) {
              int pos = r.nextInt(idx + 1);
              sampleNeighbors[i] = neighs[pos];
              neighborsTimes[i] = timestamps[pos];
            }
          } else if (idx >= count) {
            int endPos = idx >= len ? len : idx;
            int startPos = r.nextInt(endPos);

            if (startPos + count <= idx) {
              System.arraycopy(neighs, startPos, sampleNeighbors, 0, count);
              System.arraycopy(timestamps, startPos, neighborsTimes, 0, count);
            } else {
              System.arraycopy(neighs, startPos, sampleNeighbors, 0, endPos - startPos);
              System.arraycopy(neighs, 0, sampleNeighbors, endPos - startPos,
                  count - (endPos - startPos));

              System.arraycopy(timestamps, startPos, neighborsTimes, 0, endPos - startPos);
              System.arraycopy(timestamps, 0, neighborsTimes, endPos - startPos,
                  count - (endPos - startPos));
            }
          } else {
            System.out.println("time idx < 0 when count < len !");
            sampleNeighbors = new long[0];
            neighborsTimes = new int[0];
          }
        }
      }
      return new Tuple2<>(new LongNeighbor(sampleNeighbors), neighborsTimes);
    } else {
      int[] neighs = ((IntNeighbor) nodeNeighbors).getNodeIds();
      int[] sampleNeighbors;

      if (neighs == null) {
        sampleNeighbors = new int[0];
        neighborsTimes = new int[0];
      } else {
        assert (count >= 1);
        sampleNeighbors = new int[count];
        neighborsTimes = new int[count];
        // search the index of filterTimestamp
        int idx = IntArrays.binarySearch(timestamps, filterTimestamp);
        if (idx < 0) {
          idx = Math.abs(idx) - 2;
        }

        if (count >= len) {
          if (idx >= 0) {
            for (int i = 0; i < count; i++) {
              int pos = r.nextInt(idx + 1);
              sampleNeighbors[i] = neighs[pos];
              neighborsTimes[i] = timestamps[pos];
            }
          } else { // idx < 0
            System.out.println("time idx < 0 when count >= len !");
            sampleNeighbors = new int[0];
            neighborsTimes = new int[0];
          }
        } else { // count < len
          if (idx >= 0 && idx < count) {
            for (int i = 0; i < count; i++) {
              int pos = r.nextInt(idx + 1);
              sampleNeighbors[i] = neighs[pos];
              neighborsTimes[i] = timestamps[pos];
            }
          } else if (idx >= count) {
            int endPos = idx >= len ? len : idx;
            int startPos = r.nextInt(endPos);

            if (startPos + count <= idx) {
              System.arraycopy(neighs, startPos, sampleNeighbors, 0, count);
              System.arraycopy(timestamps, startPos, neighborsTimes, 0, count);
            } else {
              System.arraycopy(neighs, startPos, sampleNeighbors, 0, endPos - startPos);
              System.arraycopy(neighs, 0, sampleNeighbors, endPos - startPos,
                  count - (endPos - startPos));

              System.arraycopy(timestamps, startPos, neighborsTimes, 0, endPos - startPos);
              System.arraycopy(timestamps, 0, neighborsTimes, endPos - startPos,
                  count - (endPos - startPos));
            }
          } else {
            System.out.println("time idx < 0 when count < len!");
            sampleNeighbors = new int[0];
            neighborsTimes = new int[0];
          }
        }

      }
      return new Tuple2<>(new IntNeighbor(sampleNeighbors), neighborsTimes);
    }
  }
}
