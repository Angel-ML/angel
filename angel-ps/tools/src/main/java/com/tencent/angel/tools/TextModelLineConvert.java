package com.tencent.angel.tools;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * Text serializer
 */
public class TextModelLineConvert implements ModelLineConvert {
  private final String seperator;
  
  public TextModelLineConvert() {
    seperator = ":";
  }

  public TextModelLineConvert(String seperator) {
    this.seperator = seperator;
  }

  @Override public void convertRowIndex(FSDataOutputStream output, int rowIndex) throws
    IOException {
    output.writeBytes("rowindex=" + rowIndex + "\n");
  }

  @Override public void convertDouble(FSDataOutputStream output, int index, double value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }

  @Override public void convertFloat(FSDataOutputStream output, int index, float value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }

  @Override public void convertInt(FSDataOutputStream output, int index, float value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }

  @Override public void convertDoubleLongKey(FSDataOutputStream output, long index, double value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }
}
