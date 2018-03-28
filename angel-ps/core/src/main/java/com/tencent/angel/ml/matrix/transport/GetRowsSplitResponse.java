/*
 * Tencent is pleased to support the open source community by making Angel available.
 * 
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 * 
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * 
 * https://opensource.org/licenses/BSD-3-Clause
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.impl.matrix.*;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of get batch row splits rpc request.
 */
public class GetRowsSplitResponse extends Response {
  /** row splits */
  private List<ServerRow> rowsSplit;

  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param responseType response type
   * @param detail detail error message if the response type is error
   * @param rowsSplit row splits
   */
  public GetRowsSplitResponse(ResponseType responseType, String detail, List<ServerRow> rowsSplit) {
    super(responseType, detail);
    this.setRowsSplit(rowsSplit);
  }

  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param responseType response type
   * @param rowsSplit row splits
   */
  public GetRowsSplitResponse(ResponseType responseType, List<ServerRow> rowsSplit) {
    this(responseType, null, rowsSplit);
  }


  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param responseType response type
   * @param detail detail error message if the response type is error
   */
  public GetRowsSplitResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }

  /**
   * Create a new GetRowsSplitResponse.
   */
  public GetRowsSplitResponse() {
    this(ResponseType.SUCCESS, null, null);
  }

  /**
   * Get the row splits.
   * 
   * @return List<ServerRow> row splits
   */
  public List<ServerRow> getRowsSplit() {
    return rowsSplit;
  }

  /**
   * Set the row splits.
   * 
   * @param rowsSplit row splits
   */
  public void setRowsSplit(List<ServerRow> rowsSplit) {
    this.rowsSplit = rowsSplit;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowsSplit != null) {
      int size = rowsSplit.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowsSplit.get(i).getRowType().getNumber());
        rowsSplit.get(i).serialize(buf);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.readableBytes() == 0) {
      rowsSplit = null;
      return;
    }

    int size = buf.readInt();
    rowsSplit = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      RowType type = RowType.valueOf(buf.readInt());
      ServerRow rowSplit = null;
      switch (type) {
        case T_DOUBLE_DENSE: {
          rowSplit = new ServerDenseDoubleRow();
          break;
        }
        case T_DOUBLE_SPARSE: {
          rowSplit = new ServerSparseDoubleRow();
          break;
        }

        case T_INT_DENSE: {
          rowSplit = new ServerDenseIntRow();
          break;
        }

        case T_FLOAT_DENSE: {
          rowSplit = new ServerDenseFloatRow();
          break;
        }

        case T_INT_SPARSE: {
          rowSplit = new ServerSparseIntRow();
          break;
        }
        default:
          break;
      }

      if(rowSplit != null) {
        rowSplit.deserialize(buf);
        rowsSplit.add(rowSplit);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    if (rowsSplit != null) {
      int size = rowsSplit.size();
      for (int i = 0; i < size; i++) {
        len += rowsSplit.get(i).bufferLen();
      }
    }
    return len;
  }

  @Override
  public void clear() {
    setRowsSplit(null);
  }
}
