package com.tencent.angel.ps.server.data.profiling;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

public class ServerTimes implements Serialize {
  public volatile long receive = 0;
  public volatile long handleStart = 0;
  public volatile long deserializeRequestStart = 0;
  public volatile long partExeStart = 0;
  public volatile long allocateResponseBufStart = 0;
  public volatile long serilizeResponseStart = 0;
  public volatile long serilizeResponseOver = 0;
  public volatile long waitChannelStart = 0;
  public volatile long sendResponseStart = 0;

  /**
   * {@code System.nanoTime()} at the moment netty handed us the request bytes.
   *
   * <p>PS-local only: deliberately NOT serialized (the wire format of this class is parsed
   * by the client, adding a field would break compatibility), and nanoTime values are
   * meaningless across processes anyway. Used together with the nanoTime taken at the
   * beginning of {@code WorkerPool#handle} to measure how long the request waited in the
   * RPCWorker queue, at nanosecond resolution ({@link #receive} / {@link #handleStart} are
   * millis and truncate most queue waits to 0).
   */
  public volatile long receiveNanos = 0;

  /**
   * PS-local only (not serialized): true when the request was dispatched to a worker
   * queue, false when it was handled inline on the netty IO thread (sync path), so the
   * queue-wait statistics can separate "actually queued" from "never queued".
   */
  public volatile boolean queued = false;

  private volatile int writerStartPos = 0;

  @Override
  public void serialize(ByteBuf out) {
    writerStartPos = out.writerIndex();
    ByteBufSerdeUtils.serializeLong(out, receive);
    ByteBufSerdeUtils.serializeLong(out, handleStart);
    ByteBufSerdeUtils.serializeLong(out, deserializeRequestStart);
    ByteBufSerdeUtils.serializeLong(out, partExeStart);
    ByteBufSerdeUtils.serializeLong(out, allocateResponseBufStart);
    ByteBufSerdeUtils.serializeLong(out, serilizeResponseStart);
    ByteBufSerdeUtils.serializeLong(out, serilizeResponseOver);
    ByteBufSerdeUtils.serializeLong(out, waitChannelStart);
    ByteBufSerdeUtils.serializeLong(out, sendResponseStart);
  }

  @Override
  public void deserialize(ByteBuf in) {
    receive = ByteBufSerdeUtils.deserializeLong(in);
    handleStart = ByteBufSerdeUtils.deserializeLong(in);
    deserializeRequestStart = ByteBufSerdeUtils.deserializeLong(in);
    partExeStart = ByteBufSerdeUtils.deserializeLong(in);
    allocateResponseBufStart = ByteBufSerdeUtils.deserializeLong(in);
    serilizeResponseStart = ByteBufSerdeUtils.deserializeLong(in);
    serilizeResponseOver = ByteBufSerdeUtils.deserializeLong(in);
    waitChannelStart = ByteBufSerdeUtils.deserializeLong(in);
    sendResponseStart = ByteBufSerdeUtils.deserializeLong(in);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongLen(receive)
        + ByteBufSerdeUtils.serializedLongLen(handleStart)
        + ByteBufSerdeUtils.serializedLongLen(deserializeRequestStart)
        + ByteBufSerdeUtils.serializedLongLen(partExeStart)
        + ByteBufSerdeUtils.serializedLongLen(allocateResponseBufStart)
        + ByteBufSerdeUtils.serializedLongLen(serilizeResponseStart)
        + ByteBufSerdeUtils.serializedLongLen(serilizeResponseOver)
        + ByteBufSerdeUtils.serializedLongLen(waitChannelStart)
        + ByteBufSerdeUtils.serializedLongLen(sendResponseStart);
  }

  public void update(ByteBuf result) {
    result.setLong(writerStartPos, receive);
    //result.setLong(writerStartPos + 8, handleStart);
    //result.setLong(writerStartPos + 8 * 2, deserializeRequestStart);
    //result.setLong(writerStartPos + 8 * 3, partExeStart);
    //result.setLong(writerStartPos + 8 * 4, allocateResponseBufStart);
    //result.setLong(writerStartPos + 8 * 5, serilizeResponseStart);
    result.setLong(writerStartPos + 8 * 6, serilizeResponseOver);
    result.setLong(writerStartPos + 8 * 7, waitChannelStart);
    result.setLong(writerStartPos + 8 * 8, sendResponseStart);
  }
}
