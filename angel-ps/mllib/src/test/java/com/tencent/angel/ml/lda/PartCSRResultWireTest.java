package com.tencent.angel.ml.lda;

import com.tencent.angel.ml.lda.psf.PartCSRResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import static org.junit.Assert.*;

public class PartCSRResultWireTest {
  @Test public void lazyRowsAdvanceOuterFrameAndOwnOnlyTheirPayload() {
    ByteBuf backing = io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer();
    backing.writeInt(98765);
    ByteBuf frame = backing;
    frame.writeInt(3);
    frame.writeByte(0).writeInt(3).writeInt(1).writeInt(2).writeInt(3);
    frame.writeByte(1).writeInt(1).writeInt(2).writeInt(7);
    frame.writeByte(1).writeInt(0);
    frame.writeBoolean(true).writeInt(123456);
    backing.readInt();
    frame = backing.readRetainedSlice(backing.readableBytes());
    backing.release();
    PartCSRResult result = new PartCSRResult();
    result.deserialize(frame);
    assertTrue(frame.readBoolean());
    assertEquals(123456, frame.readInt());
    assertEquals(0, frame.readableBytes());
    frame.release();
    int[] row = new int[3];
    assertTrue(result.read(row));
    assertArrayEquals(new int[]{1, 2, 3}, row);
    assertTrue(result.read(row));
    assertArrayEquals(new int[]{0, 0, 7}, row);
    assertTrue(result.read(row));
    assertArrayEquals(new int[]{0, 0, 0}, row);
    assertFalse(result.read(row));
    result.clear();
    assertEquals(0, frame.refCnt());
  }
}
