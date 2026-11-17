package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.client.psf.universalembedding.TextUniversalEmbModelOutEmbOutputFormatWithField;
import com.tencent.angel.graph.data.FieldUpdateInfo;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.output.format.IndexAndElement;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class AdsEmbeddingUpdateOutputFormat extends TextUniversalEmbModelOutEmbOutputFormatWithField {

  long expiredSecond;

  public AdsEmbeddingUpdateOutputFormat(Configuration conf) {
    super(conf);
    expiredSecond = conf.getLong(AngelConf.ANGEL_EMBEDDING_EXPIRED_SECONDS,
        AngelConf.DEFAULT_ANGEL_EMBEDDING_EXPIRED_SECONDS);
  }

  @Override
  public IndexAndElement load(DataInputStream input) throws IOException {
    String line = input.readLine();
    IndexAndElement indexAndElement = new IndexAndElement();
    String[] keyValues = line.split(keyValueSep);

    indexAndElement.index = Long.parseLong(keyValues[0]);
    indexAndElement.element = new FieldUpdateInfo(Integer.parseInt(keyValues[1]), Long.parseLong(keyValues[2]));

    return indexAndElement;
  }

  @Override
  public void load(ServerLongAnyRow row, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowPartitionMeta rowMeta = partMeta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();

    for (int i = 0; i < elemNum; i++) {
      IndexAndElement indexAndElement = load(input);
      UniversalEmbeddingNodeWithField embeddingNodeWithField = (UniversalEmbeddingNodeWithField) row
          .get(indexAndElement.index);

      if (embeddingNodeWithField == null) {
        embeddingNodeWithField = new UniversalEmbeddingNodeWithField();
      }

      if (embeddingNodeWithField.getFieldId2UpdateTimestampSecond() == null) {
        Int2LongOpenHashMap fieldId2update = new Int2LongOpenHashMap();
        embeddingNodeWithField.setFieldId2UpdateTimestampSecond(fieldId2update);
      }

      FieldUpdateInfo info = (FieldUpdateInfo) indexAndElement.element;
      embeddingNodeWithField.getFieldId2UpdateTimestampSecond().put(info.getFieldId(), info.getUpdateTimestampSecond());

      row.set(indexAndElement.index, embeddingNodeWithField);
    }
  }

  @Override
  public void save(long key, IElement value, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();

    long count = 0;
    UniversalEmbeddingNodeWithField element = (UniversalEmbeddingNodeWithField) value;
    if (element != null) {
      Int2LongOpenHashMap updateTimeStamps = element.getFieldId2UpdateTimestampSecond();
      if (updateTimeStamps != null) {
        for (Int2LongMap.Entry entry: updateTimeStamps.int2LongEntrySet()) {
          long update = entry.getLongValue();
          if (expiredSecond < 0 || (System.currentTimeMillis() / 1000 - update) < expiredSecond) {
            // Write node id
            sb.append(key);
            sb.append(keyValueSep);
            sb.append(entry.getIntKey());
            sb.append(keyValueSep);
            sb.append(update);
            sb.append("\n");
            count += 1;
          }
        }
      }
      element.setFieldId2EmbSize(count);
    }

    output.writeBytes(sb.toString());
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
    long update =System.currentTimeMillis() / 1000 - 10;
    String path = "./test";
    Int2LongOpenHashMap fieldId2Update = new Int2LongOpenHashMap();
    fieldId2Update.put(fieldId, update);

    UniversalEmbeddingNodeWithField embedding = new UniversalEmbeddingNodeWithField(fieldId2Update);
    AdsEmbeddingUpdateOutputFormat ads = new AdsEmbeddingUpdateOutputFormat(new Configuration());

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
