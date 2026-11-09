package com.tencent.angel.graph.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for version tracking in {@link UniversalEmbeddingNodeWithField}.
 *
 * Verifies that per-(node,field) version counters are correctly incremented
 * and queried, and that version state survives deepClone.
 */
public class UniversalEmbeddingNodeWithFieldVersionTest {

    @Test
    public void testInitialVersionIsZero() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f, 2.0f}, 0);
        assertEquals(0, node.getVersion(0));
        assertEquals(0, node.getVersion(99));  // non-existent field
    }

    @Test
    public void testIncrementAndGetVersion() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f, 2.0f}, 0);
        assertEquals(1, node.incrementAndGetVersion(0));
        assertEquals(2, node.incrementAndGetVersion(0));
        assertEquals(3, node.incrementAndGetVersion(0));
    }

    @Test
    public void testIndependentFields() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f, 2.0f}, 0);
        // Add a second field
        node.getFieldId2Emb().put(1, new UniversalEmbeddingNode(new float[]{3.0f, 4.0f}));

        assertEquals(1, node.incrementAndGetVersion(0));
        assertEquals(1, node.incrementAndGetVersion(1));
        assertEquals(2, node.incrementAndGetVersion(0));
        assertEquals(1, node.getVersion(0) == 2 ? 1 : -1);  // field 1 unchanged
        assertEquals(2, node.getVersion(0));
        assertEquals(1, node.getVersion(1));
    }

    @Test
    public void testGetVersionAfterIncrements() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f}, 0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);
        assertEquals(3, node.getVersion(0));
    }

    @Test
    public void testDeepClonePreservesVersion() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f, 2.0f}, 0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);

        UniversalEmbeddingNodeWithField clone = (UniversalEmbeddingNodeWithField) node.deepClone();
        assertEquals(3, clone.getVersion(0));
    }

    @Test
    public void testDeepCloneIndependentVersion() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f}, 0);
        node.incrementAndGetVersion(0);

        UniversalEmbeddingNodeWithField clone = (UniversalEmbeddingNodeWithField) node.deepClone();
        clone.incrementAndGetVersion(0);

        assertEquals(1, node.getVersion(0));      // original unchanged
        assertEquals(2, clone.getVersion(0));     // clone incremented
    }

    @Test
    public void testStalenessScenario() {
        // Simulate the pull-push staleness scenario:
        // 1. Worker pulls embedding → version = 5
        // 2. Other workers update the same node 3 times → version = 8
        // 3. Worker pushes grad → currentVersion = 9, staleness = 9 - 5 = 4
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f}, 0);

        // Simulate 5 prior updates
        for (int i = 0; i < 5; i++) {
            node.incrementAndGetVersion(0);
        }
        int pullVersion = node.getVersion(0);  // worker captures pullVersion = 5
        assertEquals(5, pullVersion);

        // Simulate 3 updates by other workers between pull and push
        for (int i = 0; i < 3; i++) {
            node.incrementAndGetVersion(0);
        }

        // Worker pushes grad
        int currentVersion = node.incrementAndGetVersion(0);  // version = 9
        int staleness = currentVersion - pullVersion;
        assertEquals(9, currentVersion);
        assertEquals(4, staleness);  // τ = 9 - 5 = 4
    }

    @Test
    public void testVersionWithMultipleFieldsCloneAndIncrement() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f}, 0);
        node.getFieldId2Emb().put(1, new UniversalEmbeddingNode(new float[]{2.0f}));
        node.getFieldId2Emb().put(2, new UniversalEmbeddingNode(new float[]{3.0f}));

        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(1);
        node.incrementAndGetVersion(1);
        node.incrementAndGetVersion(2);
        node.incrementAndGetVersion(2);
        node.incrementAndGetVersion(2);

        UniversalEmbeddingNodeWithField clone = (UniversalEmbeddingNodeWithField) node.deepClone();
        assertEquals(1, clone.getVersion(0));
        assertEquals(2, clone.getVersion(1));
        assertEquals(3, clone.getVersion(2));

        // Increment on clone should not affect original
        clone.incrementAndGetVersion(0);
        assertEquals(1, node.getVersion(0));
        assertEquals(2, clone.getVersion(0));
    }

    @Test
    public void testBufferLenMatchesByteBufSerializeAndDeserializeBytes() {
        UniversalEmbeddingNodeWithField node = buildNodeForSerdeTest();

        ByteBuf buf = Unpooled.buffer();
        node.serialize(buf);

        int serializedBytes = buf.writerIndex();
        assertEquals(node.bufferLen(), serializedBytes);

        UniversalEmbeddingNodeWithField restored = new UniversalEmbeddingNodeWithField();
        restored.deserialize(buf);

        assertEquals(serializedBytes, buf.readerIndex());
        assertEquals(2, restored.getFieldId2Emb().size());
        assertEquals(2, restored.getFieldId2UpdateTimestampSecond().size());
        assertEquals(2, restored.getFieldId2Version().size());
        assertEquals(2, restored.getVersion(0));
        assertEquals(1, restored.getVersion(1));
    }

    @Test
    public void testBufferLenMatchesStreamSerializeAndDeserializeBytes() throws Exception {
        UniversalEmbeddingNodeWithField node = buildNodeForSerdeTest();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        node.serialize(dos);
        dos.flush();

        byte[] bytes = bos.toByteArray();
        assertEquals(node.bufferLen(), bytes.length);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        UniversalEmbeddingNodeWithField restored = new UniversalEmbeddingNodeWithField();
        restored.deserialize(dis);

        assertEquals(2, restored.getFieldId2Emb().size());
        assertEquals(2, restored.getFieldId2UpdateTimestampSecond().size());
        assertEquals(2, restored.getFieldId2Version().size());
        assertEquals(2, restored.getVersion(0));
        assertEquals(1, restored.getVersion(1));
        assertEquals(-1, dis.read());
    }

    private UniversalEmbeddingNodeWithField buildNodeForSerdeTest() {
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            new float[]{1.0f, 2.0f, 3.0f}, 0, new float[]{0.1f, 0.2f}, 2);
        node.getFieldId2Emb().put(1,
            new UniversalEmbeddingNode(new float[]{4.0f, 5.0f}, new float[]{0.3f}, 1));

        node.getFieldId2UpdateTimestampSecond().put(0, 123456789L);
        node.getFieldId2UpdateTimestampSecond().put(1, 123456799L);

        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(1);
        return node;
    }
}
