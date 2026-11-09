package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.constent.Constent;
import com.tencent.angel.graph.data.IntNeighbor;
import com.tencent.angel.graph.data.LongNeighbor;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import scala.Tuple2;

public class PartSampleNeighborResultWithTime extends PartitionGetResult {

  /**
   * Partition id
   */
  public int partId;
  public NeighborStorageType storageType;
  /**
   * Node id to edge time map
   */
  Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeIdToSampleNeighbors;

  public PartSampleNeighborResultWithTime(int partId,
      Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeIdToSampleNeighbors,
      NeighborStorageType storageType) {
    this.partId = partId;
    this.storageType = storageType;
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
  }

  public PartSampleNeighborResultWithTime() {
    this(-1, null, NeighborStorageType.LONGARRAY);
  }

  public int getPartId() {
    return partId;
  }

  public Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> getNodeIdToSampleNeighbors() {
    return nodeIdToSampleNeighbors;
  }

  public NeighborStorageType getStorageType() {
    return storageType;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeByte(output, storageType.getIndex());

    if (storageType == NeighborStorageType.LONGARRAY) {
      if (nodeIdToSampleNeighbors != null) {
        ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleNeighbors.size());
        for (Object2ObjectOpenHashMap.Entry<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> entry : nodeIdToSampleNeighbors
            .object2ObjectEntrySet()) {
          ByteBufSerdeUtils.serializeLong(output, entry.getKey()._1);
          ByteBufSerdeUtils.serializeInt(output, entry.getKey()._2);

          // neighbor
          Neighbor sampleNeighbors = entry.getValue()._1;
          if (sampleNeighbors == null) {
            sampleNeighbors = new LongNeighbor(new long[0]);
          }
          sampleNeighbors.serialize(output);

          // edge time
          int[] sampleEdgeTime = entry.getValue()._2;
          if (sampleEdgeTime == null) {
            ByteBufSerdeUtils.serializeInts(output, Constent.emptyInts);
          } else {
            ByteBufSerdeUtils.serializeInts(output, sampleEdgeTime);
          }
        }
      } else {
        ByteBufSerdeUtils.serializeInt(output, 0);
      }
    } else {
      if (nodeIdToSampleNeighbors != null) {
        ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleNeighbors.size());
        for (Object2ObjectOpenHashMap.Entry<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> entry : nodeIdToSampleNeighbors
            .object2ObjectEntrySet()) {
          ByteBufSerdeUtils.serializeLong(output, entry.getKey()._1);
          ByteBufSerdeUtils.serializeInt(output, entry.getKey()._2);

          // neighbor
          Neighbor sampleNeighbors = entry.getValue()._1;
          if (sampleNeighbors == null) {
            sampleNeighbors = new IntNeighbor(new int[0]);
          }
          sampleNeighbors.serialize(output);

          // edge time
          int[] sampleEdgeTime = entry.getValue()._2;
          if (sampleEdgeTime == null) {
            ByteBufSerdeUtils.serializeInts(output, Constent.emptyInts);
          } else {
            ByteBufSerdeUtils.serializeInts(output, sampleEdgeTime);
          }
        }
      } else {
        ByteBufSerdeUtils.serializeInt(output, 0);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
    int size = ByteBufSerdeUtils.deserializeInt(input);

    if (storageType == NeighborStorageType.LONGARRAY) {
      if (size > 0) {
        nodeIdToSampleNeighbors = new Object2ObjectOpenHashMap<>(size);
        for (int i = 0; i < size; i++) {
          long nodeId = ByteBufSerdeUtils.deserializeLong(input);
          int timestamp = ByteBufSerdeUtils.deserializeInt(input);
          LongNeighbor neighbor = new LongNeighbor();
          neighbor.deserialize(input);
          int[] neighborTime = ByteBufSerdeUtils.deserializeInts(input);
          nodeIdToSampleNeighbors
              .put(new Tuple2<>(nodeId, timestamp), new Tuple2<>(neighbor, neighborTime));
        }
      }
    } else {
      if (size > 0) {
        nodeIdToSampleNeighbors = new Object2ObjectOpenHashMap<>(size);
        for (int i = 0; i < size; i++) {
          long nodeId = ByteBufSerdeUtils.deserializeLong(input);
          int timestamp = ByteBufSerdeUtils.deserializeInt(input);
          IntNeighbor neighbor = new IntNeighbor();
          neighbor.deserialize(input);
          int[] neighborTime = ByteBufSerdeUtils.deserializeInts(input);
          nodeIdToSampleNeighbors
              .put(new Tuple2<>(nodeId, timestamp), new Tuple2<>(neighbor, neighborTime));
        }
      }
    }

  }

  @Override
  public int bufferLen() {
    int len =
        ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.BYTE_LENGTH + ByteBufSerdeUtils.INT_LENGTH;

    if (nodeIdToSampleNeighbors != null) {
      for (Object2ObjectOpenHashMap.Entry<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> entry : nodeIdToSampleNeighbors
          .object2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH + ByteBufSerdeUtils.INT_LENGTH;
        len += entry.getValue()._1.bufferLen();
        int[] sampleEdgeTime = entry.getValue()._2;
        len += ByteBufSerdeUtils.serializedIntsLen(sampleEdgeTime);
      }
    }
    return len;
  }
}
