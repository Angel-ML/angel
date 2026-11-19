package com.tencent.angel.graph.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@code deepClone()} on the universal embedding elements.
 *
 * <p>Both classes treat their arrays / maps as optional everywhere (constructors
 * leave them null, and serialize / deserialize / bufferLen all guard with a
 * presence flag), but {@code deepClone()} used to dereference them blindly:
 *
 * <ul>
 *   <li>{@code UniversalEmbeddingNode.deepClone()} read {@code slotsValues.length}
 *       (and {@code embeddings.length}) unconditionally, so it threw NPE for every
 *       node created by the {@code numSlots <= 1} init path
 *       (see {@code UniversalEmbeddingWithFieldInitAsNodes}, which calls
 *       {@code new UniversalEmbeddingNode(embeddings)}), and for every node
 *       produced by the no-arg constructor used by {@code deserialize()}.</li>
 *   <li>{@code UniversalEmbeddingNodeWithField.deepClone()} read
 *       {@code fieldId2Emb.size()} unconditionally, so it threw NPE for nodes built
 *       from the {@code (Int2LongOpenHashMap)} constructor and for anything restored
 *       by {@code deserialize()} with an empty field map.</li>
 * </ul>
 *
 * <p>Beyond "does not throw", the tests pin that <b>null stays null</b>: replacing a
 * null array with an empty one would silently change the wire format (presence flag
 * 1 + length 0 instead of flag 0) and {@code bufferLen()}. Where the serialized byte
 * order is deterministic (no field map, or a single entry) the clone's bytes are
 * compared against the original's; with several entries only {@code bufferLen()} is
 * compared, because {@code deepClone} builds the clone's fastutil map with a
 * different initial capacity and hence may iterate the entries in another order —
 * pre-existing behaviour, and harmless since {@code deserialize} rebuilds a map.
 */
public class UniversalEmbeddingNodeDeepCloneTest {

    private static byte[] serializeNode(UniversalEmbeddingNode node) {
        ByteBuf buf = Unpooled.buffer();
        try {
            node.serialize(buf);
            byte[] out = new byte[buf.writerIndex()];
            buf.getBytes(0, out);
            return out;
        } finally {
            buf.release();
        }
    }

    private static byte[] serializeNode(UniversalEmbeddingNodeWithField node) {
        ByteBuf buf = Unpooled.buffer();
        try {
            node.serialize(buf);
            byte[] out = new byte[buf.writerIndex()];
            buf.getBytes(0, out);
            return out;
        } finally {
            buf.release();
        }
    }

    private static void assertFloatsEqual(float[] expected, float[] actual, String tag) {
        assertNotNull(tag + ": expected non-null", actual);
        assertEquals(tag + ": length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(tag + ": index " + i, expected[i], actual[i], 0.0f);
        }
    }

    // ── UniversalEmbeddingNode ────────────────────────────────────────────────

    @Test
    public void testDeepCloneWithoutSlots() {
        // numSlots <= 1 init path: new UniversalEmbeddingNode(embeddings)
        UniversalEmbeddingNode node = new UniversalEmbeddingNode(new float[]{1.0f, 2.0f, 3.0f});
        assertNull("fixture must have null slots", node.getSlotsValues());

        UniversalEmbeddingNode clone = (UniversalEmbeddingNode) node.deepClone();

        assertNull("null slots must stay null in the clone", clone.getSlotsValues());
        assertFloatsEqual(new float[]{1.0f, 2.0f, 3.0f}, clone.getEmbeddings(), "no-slots emb");
        assertEquals(node.getNumSlots(), clone.getNumSlots());
        assertNotSame(node.getEmbeddings(), clone.getEmbeddings());
        assertTrue("no-slots: serialized bytes must match",
            Arrays.equals(serializeNode(node), serializeNode(clone)));
        assertEquals(node.bufferLen(), clone.bufferLen());
    }

    @Test
    public void testDeepCloneWithBothArraysNull() {
        // The no-arg constructor (used by deserialize) leaves everything null.
        UniversalEmbeddingNode node = new UniversalEmbeddingNode();

        UniversalEmbeddingNode clone = (UniversalEmbeddingNode) node.deepClone();

        assertNull(clone.getEmbeddings());
        assertNull(clone.getSlotsValues());
        assertEquals(-1, clone.getNumSlots());
        assertTrue("all-null: serialized bytes must match",
            Arrays.equals(serializeNode(node), serializeNode(clone)));
        assertEquals(node.bufferLen(), clone.bufferLen());
    }

    @Test
    public void testDeepCloneWithSlotsIsIndependent() {
        UniversalEmbeddingNode node = new UniversalEmbeddingNode(
            new float[]{1.0f, 2.0f}, new float[]{0.1f, 0.2f, 0.3f}, 3);

        UniversalEmbeddingNode clone = (UniversalEmbeddingNode) node.deepClone();
        assertTrue("with-slots: serialized bytes must match",
            Arrays.equals(serializeNode(node), serializeNode(clone)));

        node.getEmbeddings()[0] = 99.0f;
        node.getSlotsValues()[0] = 88.0f;

        assertEquals(1.0f, clone.getEmbeddings()[0], 0.0f);
        assertEquals(0.1f, clone.getSlotsValues()[0], 0.0f);
        assertEquals(3, clone.getNumSlots());
    }

    // ── UniversalEmbeddingNodeWithField ───────────────────────────────────────

    @Test
    public void testWithFieldDeepCloneWithoutSlots() {
        // The exact fixture that used to throw: slot-less embedding + version counter.
        UniversalEmbeddingNodeWithField node =
            new UniversalEmbeddingNodeWithField(new float[]{1.0f, 2.0f}, 0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);
        node.incrementAndGetVersion(0);

        UniversalEmbeddingNodeWithField clone =
            (UniversalEmbeddingNodeWithField) node.deepClone();

        assertEquals(3, clone.getVersion(0));
        assertNull(clone.getFieldId2Emb().get(0).getSlotsValues());
        assertFloatsEqual(new float[]{1.0f, 2.0f},
            clone.getFieldId2Emb().get(0).getEmbeddings(), "withField-no-slots emb");
        assertNotSame(node.getFieldId2Emb().get(0), clone.getFieldId2Emb().get(0));
        // single entry -> byte order is deterministic
        assertTrue("withField-no-slots: serialized bytes must match",
            Arrays.equals(serializeNode(node), serializeNode(clone)));
        assertEquals(node.bufferLen(), clone.bufferLen());
    }

    @Test
    public void testWithFieldDeepCloneWithNullFieldMap() {
        // The (Int2LongOpenHashMap) constructor never initialises fieldId2Emb.
        Int2LongOpenHashMap timestamps = new Int2LongOpenHashMap();
        timestamps.put(1, 123456789L);
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(timestamps);
        assertNull("fixture must have a null field map", node.getFieldId2Emb());

        UniversalEmbeddingNodeWithField clone =
            (UniversalEmbeddingNodeWithField) node.deepClone();

        assertNull("null field map must stay null in the clone", clone.getFieldId2Emb());
        assertEquals(1, clone.getFieldId2UpdateTimestampSecond().size());
        assertEquals(123456789L, clone.getFieldId2UpdateTimestampSecond().get(1));
        assertTrue("withField-null-map: serialized bytes must match",
            Arrays.equals(serializeNode(node), serializeNode(clone)));
        assertEquals(node.bufferLen(), clone.bufferLen());
    }

    @Test
    public void testWithFieldDeepCloneDefaultConstructor() {
        // The no-arg constructor parks a (-1 -> node with null embeddings AND null
        // slots) entry in the map; every node created by the init path carries it.
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField();

        UniversalEmbeddingNodeWithField clone =
            (UniversalEmbeddingNodeWithField) node.deepClone();

        assertEquals(node.getFieldId2Emb().size(), clone.getFieldId2Emb().size());
        assertNull(clone.getFieldId2Emb().get(-1).getEmbeddings());
        assertNull(clone.getFieldId2Emb().get(-1).getSlotsValues());
        assertTrue("withField-default-ctor: serialized bytes must match",
            Arrays.equals(serializeNode(node), serializeNode(clone)));
        assertEquals(node.bufferLen(), clone.bufferLen());
    }

    @Test
    public void testWithFieldDeepCloneWithNullEntryValue() {
        // The (UniversalEmbeddingNode, int) constructor accepts a null embedding.
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(
            (UniversalEmbeddingNode) null, 7);
        assertTrue(node.getFieldId2Emb().containsKey(7));
        assertNull(node.getFieldId2Emb().get(7));

        UniversalEmbeddingNodeWithField clone =
            (UniversalEmbeddingNodeWithField) node.deepClone();

        assertTrue(clone.getFieldId2Emb().containsKey(7));
        assertNull(clone.getFieldId2Emb().get(7));
    }

    @Test
    public void testWithFieldDeepCloneMixedSlotsIsIndependent() {
        Int2ObjectOpenHashMap<UniversalEmbeddingNode> fields = new Int2ObjectOpenHashMap<>();
        fields.put(0, new UniversalEmbeddingNode(new float[]{1.0f, 2.0f}));                        // no slots
        fields.put(1, new UniversalEmbeddingNode(new float[]{3.0f}, new float[]{0.5f, 0.6f}, 3));  // slots
        UniversalEmbeddingNodeWithField node = new UniversalEmbeddingNodeWithField(fields);
        node.getFieldId2UpdateTimestampSecond().put(0, 111L);
        node.incrementAndGetVersion(1);

        UniversalEmbeddingNodeWithField clone =
            (UniversalEmbeddingNodeWithField) node.deepClone();
        // several entries -> compare the length only (entry order may differ, see class doc)
        assertEquals(node.bufferLen(), clone.bufferLen());

        node.getFieldId2Emb().get(0).getEmbeddings()[0] = 99.0f;
        node.getFieldId2Emb().get(1).getSlotsValues()[0] = 88.0f;
        node.getFieldId2UpdateTimestampSecond().put(0, 222L);
        node.incrementAndGetVersion(1);

        assertEquals(1.0f, clone.getFieldId2Emb().get(0).getEmbeddings()[0], 0.0f);
        assertNull(clone.getFieldId2Emb().get(0).getSlotsValues());
        assertEquals(0.5f, clone.getFieldId2Emb().get(1).getSlotsValues()[0], 0.0f);
        assertEquals(111L, clone.getFieldId2UpdateTimestampSecond().get(0));
        assertEquals(1, clone.getVersion(1));
        assertEquals(2, node.getVersion(1));
    }
}
