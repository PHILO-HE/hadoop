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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;

/**
 * Map block to persistent memory with native PMDK libs.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativePmemMappableBlockLoader extends PmemMappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativePmemMappableBlockLoader.class);

  @Override
  CacheStats initialize(DNConf dnConf) throws IOException {
    return super.initialize(dnConf);
  }

  /**
   * Load the block.
   *
   * Map the block and verify its checksum.
   *
   * The block will be mapped to PmemDir/BlockPoolId-BlockId, in which PmemDir
   * is a persistent memory volume chosen by PmemVolumeManager.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block to persistent memory fails or
   *                       checksum fails.
   *
   * @return               The Mappable block.
   */
  @Override
  public MappableBlock load(long length, FileInputStream blockIn,
      FileInputStream metaIn, String blockFileName,
      ExtendedBlockId key)
      throws IOException {
    NativePmemMappedBlock mappableBlock = null;
    POSIX.PmemMappedRegion region = null;
    String cachePath = null;

    FileChannel blockChannel = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      assert NativeIO.isAvailable();
      cachePath = PmemVolumeManager.getInstance().getCachePath(key);
      region = POSIX.Pmem.mapBlock(cachePath, length);
      if (region == null) {
        throw new IOException("Failed to map the block " + blockFileName +
            " to persistent storage.");
      }
      // Get block path from blockFileName which includes schema.
      String blockPath = new URI(blockFileName).getPath();
      // Native code may throw RuntimeException
      POSIX.Pmem.memCopyOffHeap(blockPath, region.getAddress(),
          region.isPmem(), length);
      if (region != null) {
        POSIX.Pmem.memSync(region);
      }
      verifyChecksum(length, metaIn, new RandomAccessFile(
          cachePath, "r").getChannel(), blockFileName);
      mappableBlock = new NativePmemMappedBlock(region.getAddress(),
          region.getLength(), key);
      LOG.info("Successfully cached one replica:{} into persistent memory"
          + ", [cached path={}, address={}, length={}]", key, cachePath,
          region.getAddress(), length);
    } catch (URISyntaxException | RuntimeException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
        if (region != null) {
          // unmap content from persistent memory
          POSIX.Pmem.unmapBlock(region.getAddress(),
              region.getLength());
          FsDatasetUtil.deleteMappedFile(cachePath);
        }
      }
    }
    return mappableBlock;
  }

  @Override
  public boolean isNativeLoader() {
    return true;
  }
}
