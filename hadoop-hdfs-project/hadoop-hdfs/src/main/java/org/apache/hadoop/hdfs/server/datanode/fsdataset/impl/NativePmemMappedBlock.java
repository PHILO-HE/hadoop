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
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Represents an HDFS block that is mapped to persistent memory by the DataNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativePmemMappedBlock implements MappableBlock {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativePmemMappedBlock.class);

  private long pmemMappedAddres = -1L;
  private long length;
  private ExtendedBlockId key;

  NativePmemMappedBlock(long pmemMappedAddres, long length,
                        ExtendedBlockId key) {
    assert length > 0;
    this.pmemMappedAddres = pmemMappedAddres;
    this.length = length;
    this.key = key;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public void close() {
    if (pmemMappedAddres != -1L) {
      String cacheFilePath =
          PmemVolumeManager.getInstance().getCachePath(key);
      LOG.info("Start to unmap file " + cacheFilePath + " with length " + length +
          " from address " + pmemMappedAddres);
      // Current libpmem will report error when pmem_unmap is called with
      // length not aligned with page size, although the length is returned by
      // pmem_map_file.
      NativeIO.POSIX.Pmem.unmapBlock(pmemMappedAddres, length);
      pmemMappedAddres = -1L;
      try {
        FsDatasetUtil.deleteMappedFile(cacheFilePath);
      } catch (IOException e) {
        LOG.warn("Failed to delete the mapped File: {}!", cacheFilePath, e);
      }
    }
  }
}
