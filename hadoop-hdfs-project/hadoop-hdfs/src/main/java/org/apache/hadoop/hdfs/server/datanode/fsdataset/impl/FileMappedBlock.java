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

import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * Represents an HDFS block that is mapped to persistent memory by the DataNode.
 * PMDK is NOT involved in this implementation.
 */
public class FileMappedBlock implements MappableBlock {
  private static final Logger LOG = LoggerFactory.getLogger(FileMappedBlock
      .class);
  private static FsDatasetImpl dataset;
  private MappedByteBuffer mmap;
  private long length;
  private String filePath = null;
  private ExtendedBlockId key;

  FileMappedBlock(MappedByteBuffer mmap, long length, String filePath,
                  ExtendedBlockId key) {
    assert length > 0;
    this.mmap = mmap;
    this.length = length;
    this.filePath = filePath;
    this.key = key;
  }

  public long getLength() {
    return length;
  }

  public static void setDataset(FsDatasetImpl impl) {
    dataset = impl;
  }

  public void afterCache() {
    try {
      ReplicaInfo replica = dataset.getBlockReplica(key.getBlockPoolId(),
          key.getBlockId());
      replica.setCachePath(filePath);
    } catch (IOException e) {
      LOG.warn("Fail to find the replica file of PoolID = " +
          key.getBlockPoolId() + ", BlockID = " + key.getBlockId() +
          " for :" + e.getMessage());
    }
  }

  @Override
  public void close() {
    LOG.info("Start to unmap file " + filePath + " with length " + length);
    NativeIO.POSIX.munmap(mmap);
    FileMappableBlockLoader.deleteMappedFile(filePath);
    filePath = null;
  }
}