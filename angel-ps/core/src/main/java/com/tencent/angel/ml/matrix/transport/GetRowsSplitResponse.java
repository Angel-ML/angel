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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class GetRowsSplitResponse extends Response {
  private List<ServerRow> rowsSplit;

  public GetRowsSplitResponse(ResponseType responseType, String detail, List<ServerRow> rowsSplit) {
    super(responseType, detail);
    this.setRowsSplit(rowsSplit);
  }

  public GetRowsSplitResponse() {
    this(ResponseType.SUCCESS, null, null);
  }

  public List<ServerRow> getRowsSplit() {
    return rowsSplit;
  }

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
    rowsSplit = new ArrayList<ServerRow>();
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

      rowSplit.deserialize(buf);
      rowsSplit.add(rowSplit);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    if (rowsSplit != null) {
      int size = 0;
      for (int i = 0; i < size; i++) {
        len += rowsSplit.get(i).bufferLen();
      }
    }
    return len;
  }

}
