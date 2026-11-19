package com.tencent.angel.spark.ml.ann_core.psf.getIVFHnswVectors;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import scala.Tuple2;

public class GetIVFHnswVectorsPartResult extends PartitionGetResult {

  private Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> vec;

  public GetIVFHnswVectorsPartResult(Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> vec) {
    this.vec = vec;
  }

  public GetIVFHnswVectorsPartResult() {
    this(null);
  }

  public Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> getVec() {
    return vec;
  }

  public int size() {
    return this.vec.size();
  }

  @Override
  public void serialize(ByteBuf output) {
    int vecNum = vec.size();
    output.writeInt(vecNum);
    ObjectIterator<Long2ObjectOpenHashMap.Entry<Tuple2<long[], float[][]>>> iter = vec
        .long2ObjectEntrySet().fastIterator();
    while (iter.hasNext()) {
      Long2ObjectOpenHashMap.Entry<Tuple2<long[], float[][]>> entry = iter.next();
      output.writeLong(entry.getLongKey());
      Tuple2<long[], float[][]> value = entry.getValue();
      ByteBufSerdeUtils.serializeLongs(output, value._1);
      ByteBufSerdeUtils.serialize2DFloats(output, value._2);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    this.vec = new Long2ObjectOpenHashMap<>(len);
    for (int i = 0; i < len; i += 1) {
      long key = input.readLong();
      long[] label = ByteBufSerdeUtils.deserializeLongs(input);
      float[][] vector = ByteBufSerdeUtils.deserialize2DFloats(input);
      this.vec.put(key, new Tuple2<>(label, vector));
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    int vecNum = vec.size();
    len += 12 * vecNum;

    ObjectIterator<Long2ObjectOpenHashMap.Entry<Tuple2<long[], float[][]>>> iter = vec
        .long2ObjectEntrySet().fastIterator();
    while (iter.hasNext()) {
      Long2ObjectOpenHashMap.Entry<Tuple2<long[], float[][]>> entry = iter.next();
      Tuple2<long[], float[][]> value = entry.getValue();

      len += ByteBufSerdeUtils.serializedLongsLen(value._1);
      len += ByteBufSerdeUtils.serialized2DFloatsLen(value._2);
    }

    return len;
  }

}
