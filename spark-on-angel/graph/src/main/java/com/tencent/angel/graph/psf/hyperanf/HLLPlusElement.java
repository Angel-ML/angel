package com.tencent.angel.graph.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HLLPlusElement implements IElement {

  private static final Log LOG = LogFactory.getLog(UpdateHyperLogLogPartParam.class);
  private HyperLogLogPlus counter;

  public HLLPlusElement(HyperLogLogPlus counter) {
    this.counter = counter;
  }

  public HLLPlusElement() { this(null); }

  public HyperLogLogPlus getCounter() { return counter; }

  public void setCounter(HyperLogLogPlus counter) { this.counter =  counter; }

  @Override
  public HLLPlusElement deepClone() {
    return new HLLPlusElement(counter);
  }

  @Override
  public void serialize(ByteBuf output) {
    try {
      byte[] bytes = counter.getBytes();
      output.writeInt(bytes.length);
      output.writeBytes(bytes);
    } catch (IOException e) {
      LOG.error("Serialize failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    byte[] bytes = new byte[len];
    input.readBytes(bytes);
    try {
      counter = HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      LOG.error("ReadCounter failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public int bufferLen() {
    return (4 + counter.sizeof());
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    byte[] bytes = counter.getBytes();
    output.writeInt(bytes.length);
    output.writeBytes(new String(bytes));
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    int len = input.readInt();
    byte[] bytes = new byte[len];
    input.readFully(bytes);
    counter = HyperLogLogPlus.Builder.build(bytes);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
