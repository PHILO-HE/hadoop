/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Maps block to memory or persistent memory by DataNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class MappableBlockLoader {

  /**
   * Load the block.
   *
   * Map the block, and then verify its checksum.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream.  Should be positioned at the
   *                       start.  The caller must close this.
   * @param metaIn         The meta file input stream.  Should be positioned at
   *                       the start.  The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @return               The Mappable block.
   */
  abstract MappableBlock load(long length, FileInputStream blockIn,
                              FileInputStream metaIn, String blockFileName,
                              ExtendedBlockId key)
      throws IOException;

  /**
   * Reads bytes into a buffer until EOF or the buffer's limit is reached.
   */
  protected int fillBuffer(FileChannel channel, ByteBuffer buf)
      throws IOException {
    int bytesRead = channel.read(buf);
    if (bytesRead < 0) {
      //EOF
      return bytesRead;
    }
    while (buf.remaining() > 0) {
      int n = channel.read(buf);
      if (n < 0) {
        //EOF
        return bytesRead;
      }
      bytesRead += n;
    }
    return bytesRead;
  }
}
