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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Map block to persistent memory by using mapped byte buffer.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemMappableBlockLoader extends MappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(PmemMappableBlockLoader.class);
  private FsDatasetCache cacheManager;
  private final FsDatasetImpl dataset;
  private final ArrayList<String> pmemVolumes = new ArrayList<>();
  private int count = 0;
  // Strict atomic operation is not guaranteed for the performance sake.
  private int index = 0;

  public PmemMappableBlockLoader(FsDatasetCache cacheManager,
                                 FsDatasetImpl dataset) throws IOException {
    this.cacheManager = cacheManager;
    this.dataset = dataset;
    String[] pmemVolumesConfigured =
        dataset.datanode.getDnConf().getPmemVolumes();
    if (pmemVolumesConfigured == null || pmemVolumesConfigured.length == 0) {
      throw new IOException("The persistent memory volume, " +
          DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_DIRS_KEY +
          " is not configured!");
    }
    this.loadVolumes(pmemVolumesConfigured);
  }

  /**
   * Load and verify the configured pmem volumes.
   *
   * @throws IOException   If there is no available pmem volume.
   */
  private void loadVolumes(String[] volumes) throws IOException {
    // Check whether the directory exists
    for (String volume: volumes) {
      try {
        File pmemDir = new File(volume);
        verifyIfValidPmemVolume(pmemDir);
        // Remove all files under the volume.
        FileUtils.cleanDirectory(pmemDir);
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to parse persistent memory volume " + volume, e);
        continue;
      } catch (IOException e) {
        LOG.error("Bad persistent memory volume: " + volume, e);
        continue;
      }
      pmemVolumes.add(volume);
      LOG.info("Added persistent memory - " + volume);
    }
    count = pmemVolumes.size();
    if (count == 0) {
      throw new IOException(
          "At least one valid persistent memory volume is required!");
    }
  }

  @VisibleForTesting
  static void verifyIfValidPmemVolume(File pmemDir)
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
    String testFilePath = pmemDir.getPath() + "/.verify.pmem." + uuidStr;
    byte[] contents = uuidStr.getBytes("UTF-8");
    RandomAccessFile file = null;
    MappedByteBuffer out = null;
    try {
      file = new RandomAccessFile(testFilePath, "rw");
      out = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
          contents.length);
      if (out == null) {
        throw new IOException();
      }
      out.put(contents);
      // Forces to write data to storage device containing the mapped file
      out.force();
    } catch (Throwable t) {
      throw new IOException(
          "Exception while writing data to persistent storage dir: " +
              pmemDir, t);
    } finally {
      if (out != null) {
        out.clear();
      }
      if (file != null) {
        IOUtils.closeQuietly(file);
        NativeIO.POSIX.munmap(out);
        try {
          FsDatasetUtil.deleteMappedFile(testFilePath);
        } catch (IOException e) {
          LOG.warn("Failed to delete test file " + testFilePath +
              " from persistent memory", e);
        }
      }
    }
  }

  /**
   * Choose a persistent memory location based on a specific algorithm.
   * Currently it is a round-robin policy.
   *
   * TODO: Refine location selection policy by considering storage utilization.
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
   * The block will be mapped to PmemDir/BlockPoolId-BlockId, in which PmemDir
   * is a persistent memory volume selected by getOneLocation() method.
   *
   * @param length         The current length of the block.
   * @param blockIn        The block input stream. Should be positioned at the
   *                       start. The caller must close this.
   * @param metaIn         The meta file input stream. Should be positioned at
   *                       the start. The caller must close this.
   * @param blockFileName  The block file name, for logging purposes.
   * @param key            The extended block ID.
   *
   * @throws IOException   If mapping block fails or checksum fails.
   *
   * @return               The Mappable block.
   */
  @Override
  public MappableBlock load(long length, FileInputStream blockIn,
                            FileInputStream metaIn, String blockFileName,
                            ExtendedBlockId key)
      throws IOException {

    PmemMappedBlock mappableBlock = null;
    String filePath = null;

    FileChannel blockChannel = null;
    RandomAccessFile file = null;
    MappedByteBuffer out = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      filePath = getOneLocation() + "/" + key.getBlockPoolId() +
          "-" + key.getBlockId();
      file = new RandomAccessFile(filePath, "rw");
      out = file.getChannel().
          map(FileChannel.MapMode.READ_WRITE, 0, length);
      if (out == null) {
        throw new IOException("Failed to map the block " + blockFileName +
            " to persistent storage.");
      }
      verifyChecksumAndMapBlock(out, length, metaIn, blockChannel,
          blockFileName);
      mappableBlock = new PmemMappedBlock(out, length, filePath, key, dataset);
      LOG.info("MappableBlock [length = " + length +
          ", path = " + filePath + "] is loaded into persistent memory");
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
        if (file != null) {
          IOUtils.closeQuietly(file);
        }
        if (out != null) {
          // unmap content from persistent memory
          NativeIO.POSIX.munmap(out);
          FsDatasetUtil.deleteMappedFile(filePath);
        }
      }
    }
    return mappableBlock;
  }

  /**
   * Verifies the block's checksum meanwhile map block to persistent memory.
   * This is an I/O intensive operation.
   */
  private void verifyChecksumAndMapBlock(
      MappedByteBuffer out, long length, FileInputStream metaIn,
      FileChannel blockChannel, String blockFileName)
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
        throw new IOException("Cannot get FileChannel from " +
            "Block InputStream meta file.");
      }
      DataChecksum checksum = header.getChecksum();
      final int bytesPerChecksum = checksum.getBytesPerChecksum();
      final int checksumSize = checksum.getChecksumSize();
      final int numChunks = (8 * 1024 * 1024) / bytesPerChecksum;
      ByteBuffer blockBuf = ByteBuffer.allocate(numChunks * bytesPerChecksum);
      ByteBuffer checksumBuf = ByteBuffer.allocate(numChunks * checksumSize);
      // Verify the checksum
      int bytesVerified = 0;
      while (bytesVerified < length) {
        Preconditions.checkState(bytesVerified % bytesPerChecksum == 0,
            "Unexpected partial chunk before EOF");
        assert bytesVerified % bytesPerChecksum == 0;
        int bytesRead = fillBuffer(blockChannel, blockBuf);
        if (bytesRead == -1) {
          throw new IOException(
              "Checksum verification failed for the block " + blockFileName +
                  ": premature EOF");
        }
        blockBuf.flip();
        // Number of read chunks, including partial chunk at end
        int chunks = (bytesRead + bytesPerChecksum - 1) / bytesPerChecksum;
        checksumBuf.limit(chunks * checksumSize);
        fillBuffer(metaChannel, checksumBuf);
        checksumBuf.flip();
        checksum.verifyChunkedSums(blockBuf, checksumBuf, blockFileName,
            bytesVerified);

        // / Copy data to persistent file
        out.put(blockBuf);
        // positioning the
        bytesVerified += bytesRead;

        // Clear buffer
        blockBuf.clear();
        checksumBuf.clear();
      }
      // Forces to write data to storage device containing the mapped file
      out.force();
    } finally {
      IOUtils.closeQuietly(metaChannel);
    }
  }

  @Override
  public String getCacheCapacityConfigKey() {
    return DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_CAPACITY_KEY;
  }

  @Override
  public long getMaxBytes() {
    return cacheManager.getMaxBytesPmem();
  }

  @Override
  long reserve(long count) {
    return cacheManager.reservePmem(count);
  }

  @Override
  long release(long count) {
    return cacheManager.releasePmem(count);
  }
}
