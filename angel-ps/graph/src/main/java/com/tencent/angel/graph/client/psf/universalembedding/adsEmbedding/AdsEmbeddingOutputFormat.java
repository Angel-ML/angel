package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.flatbuffers.dollar.tensor.WeightMessage;
import com.tencent.angel.graph.client.psf.universalembedding.TextUniversalEmbModelOutEmbOutputFormatWithField;
import com.tencent.angel.graph.data.EmbeddingOrGradWithField;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.ComplexRowFormat;
import com.tencent.angel.model.output.format.IndexAndElement;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.LongElementStorage;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

public class AdsEmbeddingOutputFormat extends TextUniversalEmbModelOutEmbOutputFormatWithField {

  public AdsEmbeddingOutputFormat(Configuration conf) {
    super(conf);
  }

  @Override
  public IndexAndElement load(DataInputStream input) throws IOException {
    int typeInt = readInt(input);
    long key = readLong(input);
    int len = readInt(input);
    byte[] buffer = new byte[len];
    input.readFully(buffer);

    WeightMessage weightMessage = WeightMessage.getRootAsWeightMessage(ByteBuffer.wrap(buffer));
    FloatVector fv = weightMessage.weightsVector();
    int wLen = weightMessage.weightsLength();

    float[] weights = new float[wLen];
    if (normalized) {
      float embsSqrtSum = 0.0f;
      for (int i = 0; i < wLen; i++) {
        weights[i] = fv.get(i);
        embsSqrtSum += weights[i] * weights[i];
        if (Float.isNaN(weights[i]) || Float.isInfinite(weights[i])) {
          throw new AngelException(
              "embedding value " + weights[i] + " is invalid, please check!!! ");
        }
      }
      embsSqrtSum = (float) Math.sqrt(embsSqrtSum + 1e-8);
      for (int i = 0; i < wLen; i++) {
        weights[i] = (float) (weights[i] / (embsSqrtSum + 1e-12));
      }
    } else {
      for (int i = 0; i < wLen; i++) {
        weights[i] = fv.get(i);
        if (Float.isNaN(weights[i]) || Float.isInfinite(weights[i])) {
          throw new AngelException(
              "embedding value " + weights[i] + " is invalid, please check!!! ");
        }
      }
    }

    IndexAndElement indexAndElement = new IndexAndElement();
    indexAndElement.index = key;
    indexAndElement.element = new EmbeddingOrGradWithField(weights, weightMessage.field());

    return indexAndElement;
  }

  @Override
  public void save(long key, IElement value, DataOutputStream output) throws IOException {

    FlatBufferBuilder fb = new FlatBufferBuilder(1024);

    int count = 0;
    UniversalEmbeddingNodeWithField element = (UniversalEmbeddingNodeWithField) value;
    if (element != null) {
      for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry : element.getFieldId2Emb()
          .int2ObjectEntrySet()) {
        if (expiredSecond < 0 || (System.currentTimeMillis() / 1000 -
            element.getFieldId2UpdateTimestampSecond().get(entry.getIntKey())) < expiredSecond) {
          // Write output embeddings
          float[] embeddings = entry.getValue().getEmbeddings();
          if (embeddings != null && entry.getIntKey() != -1) {
            int embInt = WeightMessage.createWeightsVector(fb, embeddings);
            int root = WeightMessage.createWeightMessage(fb, embInt, entry.getIntKey(),
                0L, 0L, 0L, 0, 0);
            fb.finish(root);

            byte[] buffers = fb.sizedByteArray();
            output.write(int2Bytes(8)); // use to define the type of key, 8 byte: long type
            output.write(long2Bytes(key)); // key value
            output.write(int2Bytes(buffers.length)); // buffer len of weightMessage
            output.write(buffers); // byte array of weightMessage

            count += 1;
          }
        }
      }
      element.setFieldId2EmbSize(count);
    }
  }

  @Override
  public void save(String key, IElement value, DataOutputStream output) throws IOException {
    throw new AngelException("this method is not implemented!");
  }

  @Override
  public void save(IElement key, IElement value, DataOutputStream output) throws IOException {
    throw new AngelException("this method is not implemented!");
  }

  /**
   * convert int to byte array, low byte first to adapt ads api
   *
   * @param v: int value
   * @return: byte array
   */
  public byte[] int2Bytes(int v) {
    byte[] bytes = new byte[4];
    bytes[3] = (byte) ((v >>> 24) & 0xFF);
    bytes[2] = (byte) ((v >>> 16) & 0xFF);
    bytes[1] = (byte) ((v >>> 8) & 0xFF);
    bytes[0] = (byte) ((v >>> 0) & 0xFF);
    return bytes;
  }

  /**
   * convert byte array to int, low byte first to adapt ads api
   *
   * @param bytes: bytes read from input stream
   * @return: int value
   */
  public int bytes2Int(byte[] bytes) {
    if (bytes.length != 4) {
      throw new AngelException("read int failed");
    }
    return (((bytes[3] & 0xFF) << 24) |
        ((bytes[2] & 0xFF) << 16) |
        ((bytes[1] & 0xFF) << 8) |
        ((bytes[0] & 0xFF) << 0));
  }

  /**
   * convert long to byte array, low byte first to adapt ads api
   *
   * @param v: long value
   * @return: byte array
   */
  public byte[] long2Bytes(long v) {
    byte[] bytes = new byte[8];
    bytes[7] = (byte) (v >>> 56);
    bytes[6] = (byte) (v >>> 48);
    bytes[5] = (byte) (v >>> 40);
    bytes[4] = (byte) (v >>> 32);
    bytes[3] = (byte) (v >>> 24);
    bytes[2] = (byte) (v >>> 16);
    bytes[1] = (byte) (v >>> 8);
    bytes[0] = (byte) (v >>> 0);

    return bytes;
  }

  /**
   * convert byte array to long, low byte first to adapt ads api
   *
   * @param bytes: bytes read from input stream
   * @return: long value
   */
  public long bytes2Long(byte[] bytes) {
    if (bytes.length != 8) {
      throw new AngelException("read long failed");
    }

    return (((long) (bytes[7]) << 56) +
        ((long) (bytes[6] & 0xFF) << 48) +
        ((long) (bytes[5] & 0xFF) << 40) +
        ((long) (bytes[4] & 0xFF) << 32) +
        ((long) (bytes[3] & 0xFF) << 24) +
        ((bytes[2] & 0xFF) << 16) +
        ((bytes[1] & 0xFF) << 8) +
        ((bytes[0] & 0xFF) << 0));
  }

  /**
   * read int value from input stream
   *
   * @param input: input stream
   * @return: int value
   */
  public int readInt(DataInputStream input) throws IOException {
    byte[] intBytes = new byte[4];
    input.readFully(intBytes);
    return bytes2Int(intBytes);
  }

  /**
   * read long value from input stream
   *
   * @param input: input stream
   * @return: long value
   */
  public long readLong(DataInputStream input) throws IOException {
    byte[] longBytes = new byte[8];
    input.readFully(longBytes);
    return bytes2Long(longBytes);
  }

  public static void main(String[] args) {
    int fieldId = 1;
    float[] weights = {0.0f, 0.1f, 0.2f};
    long key = 100001L;
    String path = "./test";

    UniversalEmbeddingNodeWithField embedding = new UniversalEmbeddingNodeWithField(weights,
        fieldId);
    AdsEmbeddingOutputFormat ads = new AdsEmbeddingOutputFormat(new Configuration());

    DataOutputStream output = null;
    try {
      output = new DataOutputStream(new FileOutputStream(path));
      ads.save(key, embedding, output);
      output.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    DataInputStream input = null;
    try {
      input = new DataInputStream(new FileInputStream(path));
      ads.load(input);
      input.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
