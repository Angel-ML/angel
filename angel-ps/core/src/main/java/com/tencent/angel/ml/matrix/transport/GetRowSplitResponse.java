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

/**
 * The result of get row split rpc request.
 */
public class GetRowSplitResponse extends Response {
  /** row split */
  private ServerRow rowSplit;

  /**
   * Create a new GetRowSplitResponse.
   *
   * @param responseType response type
   * @param detail detail failed message if the response type is failed
   * @param rowSplit row split
   */
  public GetRowSplitResponse(ResponseType responseType, String detail, ServerRow rowSplit) {
    super(responseType, detail);
    this.rowSplit = rowSplit;
  }

  /**
   * Create a new GetRowSplitResponse.
   */
  public GetRowSplitResponse() {
    this(ResponseType.SUCCESS, null, null);
  }

  /**
   * Create a new GetRowSplitResponse.
   *
   * @param responseType response type
   * @param rowSplit row split
   */
  public GetRowSplitResponse(ResponseType responseType, ServerRow rowSplit) {
    this(responseType, null, rowSplit);
  }

  /**
   * Create a new GetRowSplitResponse.
   *
   * @param responseType response type
   * @param detail detail failed message if the response type is failed
   */
  public GetRowSplitResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }


  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowSplit != null) {
      buf.writeInt(rowSplit.getRowType().getNumber());
      rowSplit.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.readableBytes() == 0) {
      rowSplit = null;
      return;
    }

    RowType type = RowType.valueOf(buf.readInt());
    if (rowSplit == null) {
      switch (type) {
        case T_DOUBLE_DENSE: {
          rowSplit = new ServerDenseDoubleRow();
          break;
        }

        case T_FLOAT_DENSE: {
          rowSplit = new ServerDenseFloatRow();
          break;
        }

        case T_DOUBLE_SPARSE:
        case T_DOUBLE_SPARSE_COMPONENT:{
          rowSplit = new ServerSparseDoubleRow();
          break;
        }

        case T_DOUBLE_SPARSE_LONGKEY: {
          rowSplit = new ServerSparseDoubleLongKeyRow();
          break;
        }

        case T_INT_DENSE: {
          rowSplit = new ServerDenseIntRow();
          break;
        }

        case T_FLOAT_SPARSE:
        case T_FLOAT_SPARSE_COMPONENT: {
          rowSplit = new ServerSparseFloatRow();
          break;
        }

        case T_INT_SPARSE:
        case T_INT_SPARSE_COMPONENT:{
          rowSplit = new ServerSparseIntRow();
          break;
        }
        default:
          break;
      }
    }

    if(rowSplit != null) {
      rowSplit.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + (rowSplit != null ? rowSplit.bufferLen() : 0);
  }

  /**
   * Get row split.
   * 
   * @return ServerRow row split.
   */
  public ServerRow getRowSplit() {
    return rowSplit;
  }

  /**
   * Set row split.
   * 
   * @param rowSplit row split
   */
  public void setRowSplit(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }

  @Override
  public void clear() {
    setRowSplit(null);
  }
}
