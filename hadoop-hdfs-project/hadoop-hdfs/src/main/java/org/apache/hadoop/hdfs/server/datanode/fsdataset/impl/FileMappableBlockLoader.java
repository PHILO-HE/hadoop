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
 * TODO: Refine persistent location considering storage utilization
 */
public class FileMappableBlockLoader extends MappableBlockLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileMappableBlockLoader.class);
  private final ArrayList<String> pmemVolumes = new ArrayList<>();
  private final FsDatasetImpl dataset;
  private int count = 0;
  // Strict atomic operation is not guaranteed for the performance sake.
  private int index = 0;

  public FileMappableBlockLoader(FsDatasetImpl dataset)
      throws IOException {
    this.dataset = dataset;
    String[] pmemVolumes = dataset.datanode.getDnConf().getPmemVolumes();
    if (pmemVolumes == null || pmemVolumes.length == 0) {
      throw new IOException("The persistent memory volumes are not configured!");
    }
    this.loadVolumes(pmemVolumes);
  }

  /**
   * Load and verify the configured pmem volumes.
   */
  private void loadVolumes(String[] volumes) throws IOException {
    // Check whether the directory exists
    for (String location: volumes) {
      try {
        File locFile = new File(location);
        verifyIfValidPmemVolume(locFile);
        // Remove all files under the volume.
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
    String testFilePath = pmemDir.getPath() + "/.verify.pmem." + uuidStr;
    byte[] contents = uuidStr.getBytes("UTF-8");
    RandomAccessFile file = null;
    MappedByteBuffer out = null;
    try {
      file = new RandomAccessFile(testFilePath, "rw");
      out = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
          contents.length);
      if (out == null) {
        throw new IOException("Failed to map into persistent storage.");
      }
      out.put(contents);
      // Forces to write data to storage device containing the mapped file
      out.force();
    } catch (Throwable t) {
      throw new IOException(t);
    } finally {
      if (out != null) {
        out.clear();
      }
      if (file != null) {
        IOUtils.closeQuietly(file);
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
  public MappableBlock load(long length, FileInputStream blockIn,
                            FileInputStream metaIn, String blockFileName,
                            ExtendedBlockId key)
      throws IOException {

    FileMappedBlock mappableBlock = null;
    String filePath = null;

    FileChannel blockChannel = null;
    RandomAccessFile file;
    MappedByteBuffer out = null;
    try {
      blockChannel = blockIn.getChannel();
      if (blockChannel == null) {
        throw new IOException("Block InputStream has no FileChannel.");
      }

      assert NativeIO.isAvailable();
      filePath = getOneLocation() + "/" + key.getBlockPoolId() +
          "-" + key.getBlockId();
      file = new RandomAccessFile(filePath, "rw");
      out = file.getChannel().
          map(FileChannel.MapMode.READ_WRITE, 0, length);
      if (out == null) {
        throw new IOException("Fail to map the block to persistent storage.");
      }
      verifyChecksumAndMapBlock(out, length, metaIn, blockChannel,
          blockFileName);
      mappableBlock = new FileMappedBlock(out, length, filePath, key, dataset);
      LOG.info("MappableBlock [length = " + length +
          ", path = " + filePath + "] is loaded into persistent memory");
    } finally {
      IOUtils.closeQuietly(blockChannel);
      if (mappableBlock == null) {
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
          throw new IOException("checksum verification failed: premature EOF");
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
}
