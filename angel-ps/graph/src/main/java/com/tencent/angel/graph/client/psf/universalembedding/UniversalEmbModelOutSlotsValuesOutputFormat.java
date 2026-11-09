package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.IndexAndElement;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.LongElementStorage;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;

/**
 * 仅负责保存/加载 优化器 slot 状态参数 (slotsValues) 的 OutputFormat。
 *
 * <p>设计与 {@code AdsEmbeddingUpdateOutputFormat} 一致：复用 Angel 既有的
 * MatrixFormat save/load 框架，仅修改"每行"内容含义；与 embedding 主输出共用同一份
 * partition 划分、文件元数据、目录管理。当上层希望保存哪一类信息时，只需在
 * MatrixSaveContext / MatrixLoadContext 中通过 {@code formatClassName} 选择对应的
 * OutputFormat，并指定独立的 savePath / loadPath 即可。
 *
 * <p>文本行格式：
 * <pre>
 *   key{keyValueSep}slot0_0{featSep}slot0_1{featSep}...{featSep}slotK_0{featSep}slotK_1{featSep}...
 * </pre>
 * 其中 K = numSlots - 1，slot 间使用 featSep，每个 slot 内的元素也使用 featSep。
 * 当 keyValueSep 与 featSep 相同时，自动按"全 featSep 分隔"读取，逻辑与父类
 * embedding 文本格式保持一致。
 */
public class UniversalEmbModelOutSlotsValuesOutputFormat
    extends TextUniversalEmbModelOutEmbOutputFormat {

  public UniversalEmbModelOutSlotsValuesOutputFormat(Configuration conf) {
    super(conf);
  }

  /**
   * 单行解析。返回的 element 仅承载 slotsValues，embeddings 保持为 null
   * （等到加载时与已有 EmbeddingNode 合并）。
   */
  @Override
  public IndexAndElement load(DataInputStream input) throws IOException {
    String line = input.readLine();
    IndexAndElement indexAndElement = new IndexAndElement();
    String[] keyValues = line.split(keyValueSep);
    if (keyValues == null || keyValues.length < 2) {
      throw new AngelException(
          "Invalid line format: expected at least 2 fields (index and slotsValues), but got "
              + (keyValues == null ? 0 : keyValues.length) + ". Line content may be malformed.");
    }

    indexAndElement.index = Long.parseLong(keyValues[0]);

    float[] slotsValues;
    if (featSep.equals(keyValueSep)) {
      slotsValues = new float[keyValues.length - 1];
      for (int i = 0; i < slotsValues.length; i++) {
        slotsValues[i] = Float.parseFloat(keyValues[i + 1]);
        if (Float.isNaN(slotsValues[i]) || Float.isInfinite(slotsValues[i])) {
          throw new AngelException(
              "slot value " + slotsValues[i] + " is invalid, please check!!! ");
        }
      }
    } else {
      String[] slotStrs = keyValues[1].split(featSep);
      slotsValues = new float[slotStrs.length];
      for (int i = 0; i < slotsValues.length; i++) {
        slotsValues[i] = Float.parseFloat(slotStrs[i]);
        if (Float.isNaN(slotsValues[i]) || Float.isInfinite(slotsValues[i])) {
          throw new AngelException(
              "slot value " + slotsValues[i] + " is invalid, please check!!! ");
        }
      }
    }

    UniversalEmbeddingNode node = new UniversalEmbeddingNode();
    node.setSlotsValues(slotsValues);
    indexAndElement.element = node;
    return indexAndElement;
  }

  /**
   * 加载 slotsValues 并合并到已存在的 UniversalEmbeddingNode 中。
   * 若该节点尚未被 embedding format 加载，则跳过（与原 0 初始化的回退行为兼容）。
   */
  @Override
  public void load(ServerLongAnyRow row, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowPartitionMeta rowMeta = partMeta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    int numSlots = getNumSlots();

    for (int i = 0; i < elemNum; i++) {
      IndexAndElement indexAndElement = load(input);
      UniversalEmbeddingNode parsed = (UniversalEmbeddingNode) indexAndElement.element;

      // 复用此前 embedding format 写入的节点，仅更新其 slotsValues
      UniversalEmbeddingNode existed = (UniversalEmbeddingNode) row.get(indexAndElement.index);
      if (existed == null) {
        // 没有 embedding 时仍然记录 slot，避免 slot 文件为空，
        // 等待后续 embedding 加载或推理逻辑使用。
        existed = new UniversalEmbeddingNode();
        existed.setNumSlots(numSlots);
        existed.setSlotsValues(parsed.getSlotsValues());
        row.set(indexAndElement.index, existed);
      } else {
        existed.setNumSlots(numSlots);
        existed.setSlotsValues(parsed.getSlotsValues());
      }
    }
  }

  /**
   * 单行序列化：仅写出 key + slotsValues（不写 embedding）。
   */
  @Override
  public void save(long key, IElement value, DataOutputStream output) throws IOException {
    UniversalEmbeddingNode node = (UniversalEmbeddingNode) value;
    if (node == null || node.getSlotsValues() == null || node.getNumSlots() <= 1) {
      return;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(key);
    sb.append(keyValueSep);

    float[] slotsValues = node.getSlotsValues();
    int len = slotsValues.length;
    for (int i = 0; i < len; i++) {
      if (i < len - 1) {
        sb.append(slotsValues[i]).append(featSep);
      } else {
        sb.append(slotsValues[i]).append("\n");
      }
    }
    output.writeBytes(sb.toString());
  }

  /**
   * partition 级 save：复用父类的迭代框架，仅替换每行内容写法。
   * 同时统计本 partition 实际写入的条目数到 partMeta.nnz（与 embedding format 对齐）。
   */
  @Override
  public void save(ServerLongAnyRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta partMeta, DataOutputStream output) throws IOException {
    LongElementStorage storage = row.getStorage();
    ObjectIterator<Entry<IElement>> iter = storage.iterator();
    long startPos = partMeta.getStartCol();
    long count = 0;
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      UniversalEmbeddingNode node = (UniversalEmbeddingNode) entry.getValue();
      if (node == null || node.getSlotsValues() == null || node.getNumSlots() <= 1) {
        continue;
      }
      save(entry.getLongKey() + startPos, node, output);
      count += 1;
    }
    partMeta.setNnz(count);
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
   * 简单的本地保存/加载测试用例。
   * 步骤：
   *   1) 构造一个含 slotsValues 的 UniversalEmbeddingNode
   *   2) save 到本地文件，校验文件非空
   *   3) load 回来，校验 key/slotsValues 完全一致
   * 通过设置 optimizer=adam 让 numSlots=3，触发 slotsValues 的真实写入。
   */
  public static void main(String[] args) {
    long key = 100001L;
    float[] embeddings = {0.1f, 0.2f, 0.3f, 0.4f};
    // 长度对应 numSlots=3 且 embDim=4 的优化器状态：embDim * (numSlots - 1) = 8
    float[] slotsValues = {1.0f, 1.1f, 1.2f, 1.3f, 2.0f, 2.1f, 2.2f, 2.3f};
    int numSlots = 3;
    String path = "./test_slots_values";

    Configuration conf = new Configuration();
    // 让父类 getNumSlots() 返回 3，使保存逻辑生效
    conf.set(AngelConf.ANGEL_EMBEDDING_OPTIMIZER, "adam");
    // 使用与默认一致的分隔符，便于人工检视
    conf.set(AngelConf.ANGEL_EMBEDDING_FEATURE_SEP, "space");
    conf.set(AngelConf.ANGEL_EMBEDDING_KEYVALUE_SEP, "colon");

    UniversalEmbeddingNode node = new UniversalEmbeddingNode(embeddings, slotsValues, numSlots);
    UniversalEmbModelOutSlotsValuesOutputFormat fmt =
        new UniversalEmbModelOutSlotsValuesOutputFormat(conf);

    // 1. 保存
    DataOutputStream output = null;
    try {
      output = new DataOutputStream(new FileOutputStream(path));
      fmt.save(key, node, output);
      output.close();
      System.out.println("[save] write to " + path + " ok");
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    // 2. 加载并校验
    DataInputStream input = null;
    try {
      input = new DataInputStream(new FileInputStream(path));
      IndexAndElement loaded = fmt.load(input);
      input.close();

      UniversalEmbeddingNode loadedNode = (UniversalEmbeddingNode) loaded.element;
      boolean keyOk = (loaded.index == key);
      boolean slotsOk = Arrays.equals(loadedNode.getSlotsValues(), slotsValues);

      System.out.println("[load] key expected=" + key + ", actual=" + loaded.index
          + ", match=" + keyOk);
      System.out.println("[load] slotsValues expected=" + Arrays.toString(slotsValues));
      System.out.println("[load] slotsValues actual  =" + Arrays.toString(loadedNode.getSlotsValues()));
      System.out.println("[load] slotsValues match=" + slotsOk);

      if (keyOk && slotsOk) {
        System.out.println("[result] PASS");
      } else {
        System.out.println("[result] FAIL");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
