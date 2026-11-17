package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.output.format.ComplexRowFormat;
import com.tencent.angel.model.output.format.IndexAndElement;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class TextUniversalEmbModelOutEmbOutputFormat extends ComplexRowFormat {

  public String featSep = "";
  public String keyValueSep = "";
  public boolean normalized = false;
  String optimizer;

  public TextUniversalEmbModelOutEmbOutputFormat(Configuration conf) {
    super(conf);

    switch (conf.get(AngelConf.ANGEL_EMBEDDING_FEATURE_SEP,
        AngelConf.DEFAULT_ANGEL_EMBEDDING_FEATURE_SEP)) {
      case "space":
        featSep = " ";
        break;
      case "comma":
        featSep = ",";
        break;
      case "tab":
        featSep = "\t";
        break;
      case "colon":
        featSep = ":";
        break;
      case "bar":
        featSep = "|";
        break;
    }

    switch (conf.get(AngelConf.ANGEL_EMBEDDING_KEYVALUE_SEP,
        AngelConf.DEFAULT_ANGEL_EMBEDDING_KEYVALUE_SEP)) {
      case "space":
        keyValueSep = " ";
        break;
      case "comma":
        keyValueSep = ",";
        break;
      case "tab":
        keyValueSep = "\t";
        break;
      case "colon":
        keyValueSep = ":";
        break;
      case "bar":
        keyValueSep = "|";
        break;
    }

    normalized = conf.getBoolean(AngelConf.ANGEL_EMBEDDING_NORMALIZED,
        AngelConf.DEFAULT_ANGEL_EMBEDDING_NORMALIZED);
    optimizer = conf.get(AngelConf.ANGEL_EMBEDDING_OPTIMIZER,
        AngelConf.DEFAULT_ANGEL_EMBEDDING_OPTIMIZER);
  }

  /**
   * 根据 optimizer 类型返回 slot 数量。numSlots > 1 表示需要保存 slotsValues。
   * 提供给子类与同包 SlotsValues 子格式类共享判断逻辑。
   */
  protected int getNumSlots() {
    switch (optimizer) {
      case "adam":
        return 3;
      case "adagrad":
      case "momentum":
        return 2;
      case "sgd":
      default:
        return 1;
    }
  }

  @Override
  public IndexAndElement load(DataInputStream input) throws IOException {
    String line = input.readLine();
    IndexAndElement indexAndElement = new IndexAndElement();
    String[] keyValues = line.split(keyValueSep);
    float[] embeds;

    indexAndElement.index = Long.parseLong(keyValues[0]);
    if (featSep.equals(keyValueSep)) {
      embeds = new float[keyValues.length - 1];

      if (normalized) {
        float embsSqrtSum = 0.0f;
        for (int i = 0; i < embeds.length; i++) {
          embeds[i] = Float.parseFloat(keyValues[i + 1]);
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
          embeds[i] = Float.parseFloat(keyValues[i + 1]);
          if (Float.isNaN(embeds[i]) || Float.isInfinite(embeds[i])) {
            throw new AngelException(
                "embedding value " + embeds[i] + " is invalid, please check!!! ");
          }
        }
      }
    } else {
      String[] embeddings = keyValues[1].split(featSep);
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

    indexAndElement.element = new UniversalEmbeddingNode(embeds);

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
      UniversalEmbeddingNode embeddingNode = (UniversalEmbeddingNode) indexAndElement.element;

      if (numSlots > 1) {
        // 仅初始化为 0，slotsValues 的真实值由独立的 SlotsValues OutputFormat 在
        // 单独的目录加载（通过另一次 setLoadPath + setFormatClassName 的加载流程）。
        float[] slotsValues = new float[embeddingNode.getEmbeddings().length * (numSlots - 1)];
        embeddingNode.setNumSlots(numSlots);
        embeddingNode.setSlotsValues(slotsValues);
      }

      row.set(indexAndElement.index, embeddingNode);
    }
  }

  @Override
  public void save(long key, IElement value, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();

    // Write node id
    sb.append(key);
    sb.append(keyValueSep);

    // Write output feats
    float[] embeddings = ((UniversalEmbeddingNode) value).getEmbeddings();

    int len = embeddings.length;
    for (int i = 0; i < len; i++) {
      if (i < len - 1) {
        sb.append(embeddings[i]).append(featSep);
      } else {
        sb.append(embeddings[i]).append("\n");
      }
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
}
