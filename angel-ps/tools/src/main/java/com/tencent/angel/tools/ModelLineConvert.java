package com.tencent.angel.tools;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * Model convert interface
 */
public interface ModelLineConvert {
  /**
   * Serialize row index
   * @param output output stream
   * @param rowIndex row index
   * @throws IOException
   */
  void convertRowIndex(FSDataOutputStream output, int rowIndex) throws IOException;

  /**
   * Serialize a double element
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertDouble(FSDataOutputStream output, int index, double value) throws IOException;

  /**
   * Serialize a float element
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertFloat(FSDataOutputStream output, int index, float value) throws IOException;

  /**
   * Serialize a int element
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertInt(FSDataOutputStream output, int index, float value) throws IOException;

  /**
   * Serialize a double element with longkey
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertDoubleLongKey(FSDataOutputStream output, long index, double value) throws IOException;
}
