package com.tencent.angel.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serialize2 {
  /**
   * Serialize object to the Output stream.
   *
   * @param output the Netty ByteBuf
   */
  void serialize(DataOutputStream output) throws IOException;

  /**
   * Deserialize object from the input stream.
   *
   * @param input the input stream
   */
  void deserialize(DataInputStream input) throws IOException;

  /**
   * Estimate serialized data size of the object, it used to ByteBuf allocation.
   *
   * @return int serialized data size of the object
   */
  int dataSize();
}
