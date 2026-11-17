package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.data.EmbeddingOrGradWithField;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

public class TextUniversalEmbModelOutEmbOutputFormatWithField extends
        TextUniversalEmbModelOutEmbOutputFormat {

  public long expiredSecond;

  public TextUniversalEmbModelOutEmbOutputFormatWithField(Configuration conf) {
    super(conf);
    expiredSecond = conf.getLong(AngelConf.ANGEL_EMBEDDING_EXPIRED_SECONDS,
            AngelConf.DEFAULT_ANGEL_EMBEDDING_EXPIRED_SECONDS);
  }

  @Override
  public IndexAndElement load(DataInputStream input) throws IOException {
    String line = input.readLine();
    IndexAndElement indexAndElement = new IndexAndElement();
    String[] keyValues = line.split(keyValueSep);
    float[] embeds;

    indexAndElement.index = Long.parseLong(keyValues[0]);
    int fieldId = Integer.parseInt(keyValues[1]);
    if (featSep.equals(keyValueSep)) {
      embeds = new float[keyValues.length - 2];

      if (normalized) {
        float embsSqrtSum = 0.0f;
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = Float.parseFloat(keyValues[i + 2]);
          embsSqrtSum += embeds[i] * embeds[i];
          if (Float.isNaN(embeds[i]) || Float.isInfinite(embeds[i])) {
            throw new AngelException(
                    "embedding value " + embeds[i] + " is invalid, please check!!! ");
          }
        }
        embsSqrtSum = (float) Math.sqrt(embsSqrtSum + 1e-8);
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = (float) (embeds[i] / (embsSqrtSum + 1e-12));
        }
      } else {
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = Float.parseFloat(keyValues[i + 2]);
          if (Float.isNaN(embeds[i]) || Float.isInfinite(embeds[i])) {
            throw new AngelException(
                    "embedding value " + embeds[i] + " is invalid, please check!!! ");
          }
        }
      }
    } else {
      String[] embeddings = keyValues[2].split(featSep);
      embeds = new float[embeddings.length];

      if (normalized) {
        float embsSqrtSum = 0.0f;
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = Float.parseFloat(embeddings[i]);
          embsSqrtSum += embeds[i] * embeds[i];
          if (Float.isNaN(embeds[i]) || Float.isInfinite(embeds[i])) {
            throw new AngelException(
                    "embedding value " + embeds[i] + " is invalid, please check!!! ");
          }
        }
        embsSqrtSum = (float) Math.sqrt(embsSqrtSum + 1e-8);
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = (float) (embeds[i] / (embsSqrtSum + 1e-12));
        }
      } else {
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = Float.parseFloat(embeddings[i]);
          if (Float.isNaN(embeds[i]) || Float.isInfinite(embeds[i])) {
            throw new AngelException(
                    "embedding value " + embeds[i] + " is invalid, please check!!! ");
          }
        }
      }
    }

    indexAndElement.element = new EmbeddingOrGradWithField(embeds, fieldId);

    return indexAndElement;
  }

  @Override
  public void load(ServerLongAnyRow row, MatrixPartitionMeta partMeta,
                   PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowPartitionMeta rowMeta = partMeta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();

    int numSlots = getNumSlots();

    for (int i = 0; i < elemNum; i++) {
      IndexAndElement indexAndElement = load(input);
      UniversalEmbeddingNodeWithField embeddingNodeWithField = (UniversalEmbeddingNodeWithField) row
              .get(indexAndElement.index);

      EmbeddingOrGradWithField embeddingNodeUpdate = (EmbeddingOrGradWithField) indexAndElement.element;

      if (embeddingNodeWithField == null) {
        embeddingNodeWithField = new UniversalEmbeddingNodeWithField();
      }

      if (embeddingNodeWithField.getFieldId2Emb() == null) {
        Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = new Int2ObjectOpenHashMap<>();
        embeddingNodeWithField.setFieldId2Emb(fieldId2Emb);
      }

      float[] embeddings = embeddingNodeUpdate.getValues();
      int fieldId = embeddingNodeUpdate.getFieldId();

      UniversalEmbeddingNode embeddingNode;
      if (numSlots <= 1) {
        embeddingNode = new UniversalEmbeddingNode(embeddings);
      } else {
        // slotsValues 仅用 0 初始化；真实值由独立的 SlotsValues OutputFormat 加载流程恢复
        float[] slotsValues = new float[embeddings.length * (numSlots - 1)];
        embeddingNode = new UniversalEmbeddingNode(embeddings, slotsValues, numSlots);
      }
      embeddingNodeWithField.getFieldId2Emb().put(fieldId, embeddingNode);
      embeddingNodeWithField.getFieldId2UpdateTimestampSecond().put(fieldId, System.currentTimeMillis() / 1000);

      row.set(indexAndElement.index, embeddingNodeWithField);
    }
  }

  @Override
  public void save(long key, IElement value, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();

    UniversalEmbeddingNodeWithField element = (UniversalEmbeddingNodeWithField) value;

    long count = 0;
    if (element != null) {
      for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry : element.getFieldId2Emb()
              .int2ObjectEntrySet()) {
        // Write output embeddings
        float[] embeddings = entry.getValue().getEmbeddings();
        if (embeddings != null && entry.getIntKey() != -1) {
          if (expiredSecond < 0 || (System.currentTimeMillis() / 1000 -
                  element.getFieldId2UpdateTimestampSecond().get(entry.getIntKey())) < expiredSecond) {
            // Write node id
            sb.append(key);
            sb.append(keyValueSep);
            sb.append(entry.getIntKey());
            sb.append(keyValueSep);

            int len = embeddings.length;
            for (int i = 0; i < len; i++) {
              if (i < len - 1) {
                sb.append(embeddings[i]).append(featSep);
              } else {
                sb.append(embeddings[i]).append("\n");
              }
            }
            count += 1;
          }
        }
      }
      element.setFieldId2EmbSize(count);
    }

    output.writeBytes(sb.toString());
  }

  @Override
  public void save(ServerLongAnyRow row, PSMatrixSaveContext saveContext,
                   MatrixPartitionMeta partMeta, DataOutputStream output)
          throws IOException {
    LongElementStorage storage = row.getStorage();
    ObjectIterator<Entry<IElement>> iter = storage.iterator();
    long startPos = partMeta.getStartCol();
    long size = 0;
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      UniversalEmbeddingNodeWithField element = (UniversalEmbeddingNodeWithField) entry.getValue();
      save(entry.getLongKey() + startPos, entry.getValue(), output);

      if (element != null) {
        size += element.getFieldId2EmbSize();
      }
    }
    partMeta.setNnz(size);
  }

  @Override
  public void save(RowBasedPartition part, MatrixPartitionMeta partMeta,
                   PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    List<Integer> rowIds = saveContext.getRowIndexes();

    if (rowIds == null || rowIds.isEmpty()) {
      Iterator<Map.Entry<Integer, ServerRow>> iter = part.getRowsStorage().iterator();
      rowIds = new ArrayList<>();
      while (iter.hasNext()) {
        rowIds.add(iter.next().getKey());
      }
    } else {
      rowIds = filter(part, rowIds);
    }

    FSDataOutputStream dataOutputStream =
            new FSDataOutputStream(output, null, partMeta != null ? partMeta.getOffset() : 0);

    partMeta.setSaveRowNum(rowIds.size());
    for (int rowId : rowIds) {
      ServerRow row = part.getRow(rowId);
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowId, 0, 0);
      if (row != null) {
        save(part.getRow(rowId), saveContext, partMeta, output);
        rowMeta.setElementNum((int) partMeta.getNnz());
        rowMeta.setOffset(dataOutputStream.getPos());
      } else {
        rowMeta.setElementNum(0);
        rowMeta.setOffset(dataOutputStream.getPos());
      }
      partMeta.setRowMeta(rowMeta);
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

  public static void main(String[] args) {
    int fieldId = 1;
    float[] weights = {0.0f, 0.1f, 0.2f};
    long key = 100001L;
    String path = "./test";

    UniversalEmbeddingNodeWithField embedding = new UniversalEmbeddingNodeWithField(weights,
        fieldId);
    TextUniversalEmbModelOutEmbOutputFormatWithField ads = new TextUniversalEmbModelOutEmbOutputFormatWithField(new Configuration());

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
