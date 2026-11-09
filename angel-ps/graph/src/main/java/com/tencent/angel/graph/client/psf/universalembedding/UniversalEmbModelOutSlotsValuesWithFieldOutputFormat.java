package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * 带 fieldId 的 slotsValues OutputFormat。
 * 与 {@link UniversalEmbModelOutSlotsValuesOutputFormat} 设计一致，区别仅在
 * 每行额外携带 fieldId，用于在多 field embedding 场景下区分。
 *
 * <p>文本行格式：
 * <pre>
 *   key{keyValueSep}fieldId{keyValueSep}slot0{featSep}slot1{featSep}...
 * </pre>
 * 与 {@link TextUniversalEmbModelOutEmbOutputFormatWithField} 的 embedding 行
 * 完全对齐，只是数值含义换成了 slotsValues。
 */
public class UniversalEmbModelOutSlotsValuesWithFieldOutputFormat
    extends TextUniversalEmbModelOutEmbOutputFormatWithField {

  public UniversalEmbModelOutSlotsValuesWithFieldOutputFormat(Configuration conf) {
    super(conf);
    expiredSecond = conf.getLong(AngelConf.ANGEL_EMBEDDING_EXPIRED_SECONDS,
        AngelConf.DEFAULT_ANGEL_EMBEDDING_EXPIRED_SECONDS);
  }

  /**
   * 单行解析。返回的 element 是仅含 slotsValues 的 EmbeddingNode；
   * fieldId 暂存到外层临时载体 {@link FieldSlotsValues} 中。
   */
  @Override
  public IndexAndElement load(DataInputStream input) throws IOException {
    String line = input.readLine();
    IndexAndElement indexAndElement = new IndexAndElement();
    String[] keyValues = line.split(keyValueSep);
    if (keyValues.length < 2) {
      throw new AngelException(
          "Invalid line format: expected at least 2 fields (index and fieldId), but got "
              + keyValues.length + ". Line content may be malformed.");
    }

    long parsedIndex;
    int fieldId;
    try {
      parsedIndex = Long.parseLong(keyValues[0]);
      fieldId = Integer.parseInt(keyValues[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse values from keyValues. keyValues[0]="
          + keyValues[0] + ", keyValues[1]=" + keyValues[1], e);
    }
    indexAndElement.index = parsedIndex;

    float[] slotsValues;
    if (featSep.equals(keyValueSep)) {
      slotsValues = new float[keyValues.length - 2];
      for (int i = 0; i < slotsValues.length; i++) {
        try {
          slotsValues[i] = Float.parseFloat(keyValues[i + 2]);
        } catch (NumberFormatException e) {
          throw new AngelException("Invalid float value: " + keyValues[i + 2], e);
        }
        if (Float.isNaN(slotsValues[i]) || Float.isInfinite(slotsValues[i])) {
          throw new AngelException(
              "slot value " + slotsValues[i] + " is invalid, please check!!! ");
        }
      }
    } else {
      String[] slotStrs = keyValues[2].split(featSep);
      slotsValues = new float[slotStrs.length];
      for (int i = 0; i < slotsValues.length; i++) {
        try {
          slotsValues[i] = Float.parseFloat(slotStrs[i]);
        } catch (NumberFormatException e) {
          throw new AngelException(
              "Invalid float value: " + slotStrs[i], e);
        }
        if (Float.isNaN(slotsValues[i]) || Float.isInfinite(slotsValues[i])) {
          throw new AngelException(
              "slot value " + slotsValues[i] + " is invalid, please check!!! ");
        }
      }
    }

    indexAndElement.element = new FieldSlotsValues(fieldId, slotsValues);
    return indexAndElement;
  }

  /**
   * 加载 (nodeId, fieldId) -> slotsValues 并合并到已存在的
   * {@link UniversalEmbeddingNodeWithField} 中。
   */
  @Override
  public void load(ServerLongAnyRow row, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowPartitionMeta rowMeta = partMeta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    int numSlots = getNumSlots();

    for (int i = 0; i < elemNum; i++) {
      IndexAndElement indexAndElement = load(input);
      FieldSlotsValues fsv = (FieldSlotsValues) indexAndElement.element;

      UniversalEmbeddingNodeWithField nodeWithField =
          (UniversalEmbeddingNodeWithField) row.get(indexAndElement.index);
      if (nodeWithField == null) {
        nodeWithField = new UniversalEmbeddingNodeWithField();
      }
      if (nodeWithField.getFieldId2Emb() == null) {
        nodeWithField.setFieldId2Emb(new Int2ObjectOpenHashMap<UniversalEmbeddingNode>());
      }

      Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = nodeWithField.getFieldId2Emb();
      UniversalEmbeddingNode emb = fieldId2Emb.get(fsv.fieldId);
      if (emb == null) {
        emb = new UniversalEmbeddingNode();
        fieldId2Emb.put(fsv.fieldId, emb);
      }
      emb.setNumSlots(numSlots);
      emb.setSlotsValues(fsv.slotsValues);

      row.set(indexAndElement.index, nodeWithField);
    }
  }

  /**
   * 单行序列化：仅写出 key + fieldId + slotsValues（不写 embedding）。
   * 复用父类的过期判断 expiredSecond。
   */
  @Override
  public void save(long key, IElement value, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();

    UniversalEmbeddingNodeWithField element = (UniversalEmbeddingNodeWithField) value;
    long count = 0;
    if (element != null && element.getFieldId2Emb() != null) {
      long nowSec = System.currentTimeMillis() / 1000;
      for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry : element.getFieldId2Emb()
          .int2ObjectEntrySet()) {
        int fieldId = entry.getIntKey();
        UniversalEmbeddingNode node = entry.getValue();
        if (fieldId == -1 || node == null || node.getSlotsValues() == null
            || node.getNumSlots() <= 1) {
          continue;
        }
        if (expiredSecond >= 0
            && (nowSec - element.getFieldId2UpdateTimestampSecond().get(fieldId)) >= expiredSecond) {
          continue;
        }

        sb.append(key);
        sb.append(keyValueSep);
        sb.append(fieldId);
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
        count += 1;
      }
      element.setFieldId2EmbSize(count);
    }

    output.writeBytes(sb.toString());
  }

  @Override
  public void save(ServerLongAnyRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta partMeta, DataOutputStream output) throws IOException {
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

  /**
   * 与父类对齐 RowBasedPartition 级 save 流程，保证 partMeta/rowMeta
   * 中的 elementNum 与实际写入数据一一对应。
   */
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

  /**
   * 用于在 load 阶段单行解析时承载 (fieldId, slotsValues) 的临时载体。
   * 不实现 IElement 的真实业务（仅作 IndexAndElement 占位），与
   * {@code EmbeddingOrGradWithField}、{@code FieldUpdateInfo} 等的用法一致。
   */
  private static class FieldSlotsValues implements IElement {
    final int fieldId;
    final float[] slotsValues;

    FieldSlotsValues(int fieldId, float[] slotsValues) {
      this.fieldId = fieldId;
      this.slotsValues = slotsValues;
    }

    @Override public Object deepClone() { return this; }
    @Override public void serialize(io.netty.buffer.ByteBuf output) { }
    @Override public void deserialize(io.netty.buffer.ByteBuf input) { }
    @Override public int bufferLen() { return 0; }
    @Override public void serialize(DataOutputStream output) { }
    @Override public void deserialize(DataInputStream input) { }
    @Override public int dataLen() { return 0; }
  }

  /**
   * 简单的本地保存/加载测试用例。
   * 步骤：
   *   1) 构造一个含两个 fieldId 的 UniversalEmbeddingNodeWithField，
   *      每个 fieldId 都带不同的 slotsValues
   *   2) save 到本地文件
   *   3) 通过反复调用 load(DataInputStream) 逐行解析，重建 (fieldId -> slotsValues) 映射
   *   4) 校验所有 key/fieldId/slotsValues 均与原始数据一致
   * 通过设置 optimizer=adam 让 numSlots=3，确保 slotsValues 实际写入。
   */
  public static void main(String[] args) {
    long key = 100001L;
    int fieldIdA = 1;
    int fieldIdB = 2;
    float[] embA = {0.0f, 0.1f, 0.2f, 0.3f};
    float[] embB = {0.4f, 0.5f, 0.6f, 0.7f};
    float[] slotsA = {1.0f, 1.1f, 1.2f, 1.3f, 2.0f, 2.1f, 2.2f, 2.3f};
    float[] slotsB = {3.0f, 3.1f, 3.2f, 3.3f, 4.0f, 4.1f, 4.2f, 4.3f};
    int numSlots = 3;
    String path = "./test_slots_values_with_field";

    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_EMBEDDING_OPTIMIZER, "adam");
    conf.set(AngelConf.ANGEL_EMBEDDING_FEATURE_SEP, "space");
    conf.set(AngelConf.ANGEL_EMBEDDING_KEYVALUE_SEP, "colon");
    // expiredSecond 默认 -1，永不过期；这里显式设置以保证测试稳定
    conf.setLong(AngelConf.ANGEL_EMBEDDING_EXPIRED_SECONDS, -1L);

    UniversalEmbeddingNodeWithField nodeWithField = new UniversalEmbeddingNodeWithField();
    Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = new Int2ObjectOpenHashMap<>();
    fieldId2Emb.put(fieldIdA, new UniversalEmbeddingNode(embA, slotsA, numSlots));
    fieldId2Emb.put(fieldIdB, new UniversalEmbeddingNode(embB, slotsB, numSlots));
    nodeWithField.setFieldId2Emb(fieldId2Emb);

    UniversalEmbModelOutSlotsValuesWithFieldOutputFormat fmt =
        new UniversalEmbModelOutSlotsValuesWithFieldOutputFormat(conf);

    // 1. 保存
    DataOutputStream output = null;
    try {
      output = new DataOutputStream(new FileOutputStream(path));
      fmt.save(key, nodeWithField, output);
      System.out.println("[save] write to " + path + " ok, count="
          + nodeWithField.getFieldId2EmbSize());
    } catch (Exception e) {
      e.printStackTrace();
      return;
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (Exception ignore) {
          // ignore
        }
      }
    }

    // 2. 加载并按行解析
    Int2ObjectOpenHashMap<float[]> loadedFieldSlots = new Int2ObjectOpenHashMap<>();
    long loadedKey = -1L;
    try (DataInputStream input = new DataInputStream(new FileInputStream(path))) {
      // 测试数据中有两个 fieldId，所以应能解析出两行
      for (int i = 0; i < 2; i++) {
        IndexAndElement loaded = fmt.load(input);
        FieldSlotsValues fsv = (FieldSlotsValues) loaded.element;
        loadedKey = loaded.index;
        loadedFieldSlots.put(fsv.fieldId, fsv.slotsValues);
        System.out.println("[load] line " + i + ": key=" + loaded.index
            + ", fieldId=" + fsv.fieldId
            + ", slots=" + Arrays.toString(fsv.slotsValues));
      }
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    // 3. 校验
    boolean keyOk = (loadedKey == key);
    boolean fieldACountOk = loadedFieldSlots.containsKey(fieldIdA);
    boolean fieldBCountOk = loadedFieldSlots.containsKey(fieldIdB);
    boolean slotsAOk = fieldACountOk
        && Arrays.equals(loadedFieldSlots.get(fieldIdA), slotsA);
    boolean slotsBOk = fieldBCountOk
        && Arrays.equals(loadedFieldSlots.get(fieldIdB), slotsB);

    System.out.println("[check] key match           = " + keyOk);
    System.out.println("[check] fieldA(" + fieldIdA + ") slots match = " + slotsAOk);
    System.out.println("[check] fieldB(" + fieldIdB + ") slots match = " + slotsBOk);

    if (keyOk && slotsAOk && slotsBOk) {
      System.out.println("[result] PASS");
    } else {
      System.out.println("[result] FAIL");
    }
  }
}
