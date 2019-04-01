package com.tencent.angel.model.output.format.element;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface ISerialize {
  void serialize(DataOutputStream out) throws IOException;
  void deserialize(DataInputStream in) throws IOException;
}
