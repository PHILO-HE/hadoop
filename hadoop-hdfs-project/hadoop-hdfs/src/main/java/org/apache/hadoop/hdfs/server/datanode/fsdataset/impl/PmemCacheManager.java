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

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Manage the non-volatile storage class memory cache volumes.
 *
 * TODO: Refine persistent location considering storage utilization
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemCacheManager implements MappableBlockLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PmemCacheManager
      .class);
  private final ArrayList<String> pmemVolumes = new ArrayList<String>();
  // It's not a strict atomic operation for the performance sake.
  private int index = 0;
  private int count = 0;

  public PmemCacheManager(String[] pmemVolumes) throws IOException {
    this.loadVolumes(pmemVolumes);
  }

  private void loadVolumes(String[] volumes) throws IOException {
    // Check whether the directory exists
    for (String location: volumes) {
      try {
        File locFile = new File(location);
        verifyIfValidPmemVolume(locFile);
        // Remove all files under the volume. Files may be left after an
        // unexpected datanode restart.
        FileUtils.cleanDirectory(locFile);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse persistent memory location " + location +
            " for " + e.getMessage());
        throw new IOException(e);
      }
      pmemVolumes.add(location);
      LOG.info("Added persistent memory - " + location);
    }
    count = pmemVolumes.size();
  }

  public static void verifyIfValidPmemVolume(File pmemDir)
      throws IOException {
    if (!pmemDir.exists()) {
      final String message = pmemDir + " does not exist";
      throw new IOException(message);
    }

    if (!pmemDir.isDirectory()) {
      final String message = pmemDir + " is not a directory";
      throw new IllegalArgumentException(message);
    }

    String uuidStr = UUID.randomUUID().toString();
    String testFile = pmemDir.getPath() + "/.verify.pmem." + uuidStr;
    byte[] contents = uuidStr.getBytes("UTF-8");
    NativeIO.POSIX.PmemMappedRegion region = null;
    try {
      region = NativeIO.POSIX.Pmem.mapBlock(testFile, contents.length);
      if (region == null) {
        throw new IOException("Failed to map into persistent storage.");
      }
      NativeIO.POSIX.Pmem.memCopy(contents, region.getAddress(), region.isPmem(),
          contents.length);
      NativeIO.POSIX.Pmem.memSync(region);
    } catch (Throwable t) {
      throw new IOException(t);
    } finally {
      if (region != null) {
        NativeIO.POSIX.Pmem.unmapBlock(region.getAddress(), region.getLength());
        boolean deleted = false;
        String reason = null;
        try {
          deleted = new File(testFile).delete();
        } catch (Throwable t) {
          reason = t.getMessage();
        }
        if (!deleted) {
          LOG.warn("Failed to delete persistent memory test file " +
              testFile + (reason == null ? "" : " due to: " + reason));
        }
      }
    }
  }

  /**
   * Choose a persistent location based on specific algorithms.
   * Currently it is a round-robin policy.
   */
  public String getOneLocation() {
    if (count != 0) {
      return pmemVolumes.get(index++ % count);
    } else {
      throw new RuntimeException("No usable persistent memory are found");
    }
  }

  /**
   * Load the block.
   *
   * Map the block and verify its checksum.
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
  @Override
  public MappableBlock loadMappableBlock(long length, FileInputStream blockIn,
                                   FileInputStream metaIn, String blockFileName, ExtendedBlockId key)
      throws IOException {

    PmemMappedBlock mappableBlock = null;
    NativeIO.POSIX.PmemMappedRegion region = null;
    String filePath = null;

    FileChannel blockChannel = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      assert NativeIO.isAvailable();
      filePath = getOneLocation() + "/" + key.getBlockPoolId() +
          "-" + key.getBlockId();
      region = NativeIO.POSIX.Pmem.mapBlock(filePath, length);
      if (region == null) {
        throw new IOException("Fail to map the block to persistent storage.");
      }
      verifyChecksum(region, length, metaIn, blockChannel, blockFileName);
      mappableBlock = new PmemMappedBlock(region.getAddress(),
          region.getLength(), filePath, key);
      LOG.info("MappableBlock with address = " + region.getAddress() +
          ", length = " + region.getLength() + ", path = " + filePath +
          " in persistent memory");
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
        if (region != null) {
          // unmap content from persistent memory
          NativeIO.POSIX.Pmem.unmapBlock(region.getAddress(),
              region.getLength());
          deleteMappedFile(filePath);
        }
      }
    }
    return mappableBlock;
  }

  /**
   * Verifies the block's checksum meanwhile copy the block data to pmem.
   * This is an I/O intensive operation.
   */
  private static void verifyChecksum(NativeIO.POSIX.PmemMappedRegion region, long length,
                                     FileInputStream metaIn, FileChannel blockChannel, String blockFileName)
      throws IOException {
    // Verify the checksum from the block's meta file
    // Get the DataChecksum from the meta file header
    BlockMetadataHeader header =
        BlockMetadataHeader.readHeader(new DataInputStream(
            new BufferedInputStream(metaIn, BlockMetadataHeader
                .getHeaderSize())));
    FileChannel metaChannel = null;
    try {
      metaChannel = metaIn.getChannel();
      if (metaChannel == null) {
        throw new IOException("Block InputStream meta file has no FileChannel.");
      }
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      // Verify the checksum
      int bytesVerified = 0;
      long mappedAddress = -1L;
      if (region != null) {
        mappedAddress = region.getAddress();
      }
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF");
        assert bytesVerified % bytesPerChecksum == 0;
        int bytesRead = MappableBlockLoader.fillBuffer(blockChannel, blockBuf);
        if (bytesRead == -1) {
          throw new IOException("checksum verification failed: premature EOF");
        }
        blockBuf.flip();
        // Number of read chunks, including partial chunk at end
        int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.limit(chunks * checksumSize);
        MappableBlockLoader.fillBuffer(metaChannel, checksumBuf);
        checksumBuf.flip();
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);
        // Success
        bytesVerified += bytesRead;
        // Copy data to persistent file
        NativeIO.POSIX.Pmem.memCopy(blockBuf.array(), mappedAddress,
            region.isPmem(), bytesRead);
        mappedAddress += bytesRead;
        // Clear buffer
        blockBuf.clear();
        checksumBuf.clear();
      }
      if (region != null) {
        NativeIO.POSIX.Pmem.memSync(region);
      }
    } finally {
      IOUtils.closeQuietly(metaChannel);
    }
  }

  public static void deleteMappedFile(String filePath) {
    try {
      if (filePath != null) {
        boolean result = Files.deleteIfExists(Paths.get(filePath));
        if (!result) {
          LOG.error("Fail to delete mapped file " + filePath +
              " from persistent memory");
        }
      }
    } catch (Throwable e) {
      LOG.error("Fail to delete mapped file " + filePath + " for " +
          e.getMessage() + " from persistent memory");
    }
  }
}
