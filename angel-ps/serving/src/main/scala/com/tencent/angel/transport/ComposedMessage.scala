/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.transport

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import io.netty.buffer.ByteBuf
import io.netty.channel.FileRegion
import io.netty.util.AbstractReferenceCounted

/**
  * A wrapper message that holds two separate pieces (a header and a body).
  *
  * The header must be a ByteBuf, while the body can be a ByteBuf or a FileRegion.
  */
object ComposedMessage {
  /**
    * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
    * The size should not be too large as it will waste underlying memory copy. e.g. If network
    * avaliable buffer is smaller than this limit, the data cannot be sent within one single write
    * operation while it still will make memory copy with this size.
    */
  val NIO_BUFFER_LIMIT = 256 * 1024
}

/**
  * Construct a new ComposedMessage.
  *
  * @param header     the message header.
  * @param body       the message body.
  * @param bodyLength the length of the message body, in bytes.
  */
class ComposedMessage private[transport](val header: ByteBuf, val body: Any, val bodyLength: Int) extends AbstractReferenceCounted with FileRegion {
  private val headerLength = header.readableBytes
  private var totalBytesTransferred = 0L

  override def count: Long = headerLength + bodyLength

  override def position = 0

  /**
    * This code is more complicated than you would think because we might require multiple transferTo
    * invocations in order to transfer a single MessageWithHeader to avoid busy waiting.
    *
    * The contract is that the caller will ensure position is properly set to the total number of
    * bytes transferred so far (i.e. value returned by transfered()).
    */
  @throws[IOException]
  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    require(position == totalBytesTransferred, "Invalid position.")
    // Bytes written for header in this call.
    var writtenHeader = 0
    if (header.readableBytes > 0) {
      writtenHeader = copyByteBuf(header, target)
      totalBytesTransferred += writtenHeader
      if (header.readableBytes > 0) return writtenHeader
    }
    var writtenBody = 0L
    if (body.isInstanceOf[ByteBuf]) {
      // Bytes written for body in this call.
      writtenBody = copyByteBuf(body.asInstanceOf[ByteBuf], target)
    } else {
      writtenBody = body.asInstanceOf[FileRegion].transferTo(target, totalBytesTransferred - headerLength)
    }
    totalBytesTransferred += writtenBody
    writtenHeader + writtenBody
  }

  override protected def deallocate(): Unit = {
    header.release
    if (body.isInstanceOf[ByteBuf]) {
      body.asInstanceOf[ByteBuf].release
    }
  }

  @throws[IOException]
  private def copyByteBuf(buf: ByteBuf, target: WritableByteChannel) = {
    val buffer = buf.nioBuffer
    val written = if (buffer.remaining <= ComposedMessage.NIO_BUFFER_LIMIT) target.write(buffer)
    else writeNioBuffer(target, buffer)
    buf.skipBytes(written)
    written
  }


  override def transfered(): Long = transferred

  override def transferred: Long = totalBytesTransferred

  @throws[IOException]
  private def writeNioBuffer(writeCh: WritableByteChannel, buf: ByteBuffer) = {
    val originalLimit = buf.limit
    var ret = 0
    try {
      val ioSize = Math.min(buf.remaining, ComposedMessage.NIO_BUFFER_LIMIT)
      buf.limit(buf.position + ioSize)
      ret = writeCh.write(buf)
    } finally buf.limit(originalLimit)
    ret
  }

  override def retain: FileRegion = {
    super.retain
    this
  }

  override def retain(increment: Int): FileRegion = {
    super.retain(increment)
    this
  }

  override def touch: FileRegion = this

  override def touch(hint: Any): FileRegion = this
}
