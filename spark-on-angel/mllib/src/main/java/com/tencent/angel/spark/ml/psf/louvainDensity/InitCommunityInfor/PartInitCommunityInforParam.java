package com.tencent.angel.spark.ml.psf.louvainDensity.InitCommunityInfor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

/**
 * part community's information in ps
 */
public class PartInitCommunityInforParam extends PartitionUpdateParam {

  private Long2ObjectMap<CommunityInforElement> communityId2Infor;

  private long[] communityIds;
  private transient int startIndex;
  private transient int endIndex;

  public PartInitCommunityInforParam(int matrixId, PartitionKey partitionKey,
      Long2ObjectMap<CommunityInforElement> communityId2Infor, long[] communityIds, int startIndex,
      int endIndex) {

    super(matrixId, partitionKey);
    this.communityId2Infor = communityId2Infor;
    this.communityIds = communityIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitCommunityInforParam() {
    this(0, null, null, null, 0, 0);
  }

  private void clear() {
    communityId2Infor = null;
    communityIds = null;
    startIndex = -1;
    endIndex = -1;
  }

  public Long2ObjectMap<CommunityInforElement> getCommunityId2Infor() {
    return communityId2Infor;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);

    long commId;
    CommunityInforElement infor;
    int writeIndex = buf.writerIndex();
    int numCommunitiessWriten = 0;

    buf.writeInt(0);

    for (int i = startIndex; i < endIndex; i++) {
      // get community Id
      commId = communityIds[i];
      //get  data the nodeId mapped 1.neighbors 2.tags 3.attrs
      infor = communityId2Infor.get(commId);
      if (infor == null) {
        continue;
      }
      buf.writeLong(commId);
      buf.writeLong(infor.getNumberSelfEdge());
      buf.writeLong(infor.getSize());
      buf.writeFloat(infor.getWeightDegree());
      buf.writeFloat(infor.getSelfWeight());

      numCommunitiessWriten++;
    }

    buf.setInt(writeIndex, numCommunitiessWriten);
//        clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    int len = buf.readInt();
    communityId2Infor = new Long2ObjectArrayMap<>(len);

    for (int i = 0; i < len; i++) {
      long commId = buf.readLong();
      long numberSelfEdge = buf.readLong();
      long size = buf.readLong();
      float weightDegree = buf.readFloat();
      float selfWeight = buf.readFloat();

      CommunityInforElement communityElem = new CommunityInforElement(numberSelfEdge, size,
          weightDegree, selfWeight);
      communityId2Infor.put(commId, communityElem);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      CommunityInforElement communityElem = communityId2Infor.get(communityIds[i]);
      if (communityElem != null) {
        len += 8;//community Id
        len += Long.BYTES * 2 + Float.BYTES * 2;
      }
    }
    return len;
  }
}
