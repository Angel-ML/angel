package com.tencent.angel.model.output.serde;

import com.tencent.angel.model.output.element.IntDoublesCol;
import com.tencent.angel.model.output.element.IntFloatsCol;
import com.tencent.angel.model.output.element.IntIntsCol;
import com.tencent.angel.model.output.element.IntLongsCol;
import com.tencent.angel.model.output.element.LongDoublesCol;
import com.tencent.angel.model.output.element.LongFloatsCol;
import com.tencent.angel.model.output.element.LongIntsCol;
import com.tencent.angel.model.output.element.LongLongsCol;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Column first format for matrix
 */
public interface ColumnSerDe {

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key float value matrix
   * @param output output stream
   */
  void serialize(IntFloatsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key double value matrix
   * @param output output stream
   */
  void serialize(IntDoublesCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key int value matrix
   * @param output output stream
   */
  void serialize(IntIntsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key long value matrix
   * @param output output stream
   */
  void serialize(IntLongsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key float value matrix
   * @param output output stream
   */
  void serialize(LongFloatsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key float double matrix
   * @param output output stream
   */
  void serialize(LongDoublesCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key int value matrix
   * @param output output stream
   */
  void serialize(LongIntsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key long value matrix
   * @param output output stream
   */
  void serialize(LongLongsCol col, DataOutputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key float value matrix
   * @param output input stream
   */
  void deserialize(IntFloatsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key double value matrix
   * @param output input stream
   */
  void deserialize(IntDoublesCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key int value matrix
   * @param output input stream
   */
  void deserialize(IntIntsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key long value matrix
   * @param output input stream
   */
  void deserialize(IntLongsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key float value matrix
   * @param output input stream
   */
  void deserialize(LongFloatsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key float value matrix
   * @param output input stream
   */
  void deserialize(LongDoublesCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key int value matrix
   * @param output input stream
   */
  void deserialize(LongIntsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key long value matrix
   * @param output input stream
   */
  void deserialize(LongLongsCol col, DataInputStream output) throws IOException;
}
