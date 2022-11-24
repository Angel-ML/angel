package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.model.output.format.ComplexRowFormat;
import com.tencent.angel.model.output.format.IndexAndElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class TextUniversalEmbModelOutEmbOutputFormat extends ComplexRowFormat {
  String featSep = "";
  String keyValueSep = "";

  public TextUniversalEmbModelOutEmbOutputFormat(Configuration conf) {
    super(conf);

    switch(conf.get("angel.embedding.feature.sep", "space")) {
      case "space" : featSep = " "; break;
      case "comma" : featSep = ","; break;
      case "tab" : featSep = "\t"; break;
      case "colon" : featSep = ":"; break;
      case "bar" : featSep = "|"; break;
    }

    switch(conf.get("angel.embedding.keyvalue.sep", "colon")) {
      case "space" : keyValueSep = " "; break;
      case "comma" : keyValueSep = ","; break;
      case "tab" : keyValueSep = "\t"; break;
      case "colon" : keyValueSep = ":"; break;
      case "bar" : keyValueSep = "|"; break;
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

      for (int i = 0; i < embeds.length; i++) {
        embeds[i] = Float.parseFloat(keyValues[i + 1]);
      }
    } else {
      String[] embeddings = keyValues[1].split(featSep);
      embeds = new float[embeddings.length];

      for (int i = 0; i < embeddings.length; i++) {
        embeds[i] = Float.parseFloat(embeddings[i]);
      }
    }

    indexAndElement.element = new UniversalEmbeddingNode(embeds);

    return indexAndElement;
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
