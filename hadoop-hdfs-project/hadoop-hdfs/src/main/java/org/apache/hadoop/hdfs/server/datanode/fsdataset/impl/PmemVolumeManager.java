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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Manage the persistent memory volumes.
 */
public class PmemVolumeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(PmemVolumeManager.class);
  private final ArrayList<String> pmemVolumes = new ArrayList<>();
  private int count = 0;
  // Strict atomic operation is not guaranteed for the performance sake.
  private int index = 0;

  public PmemVolumeManager(FsDatasetImpl dataset) throws IOException {
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
  public String getOneLocation() throws IOException {
    if (count != 0) {
      return pmemVolumes.get(index++ % count);
    } else {
      throw new IOException("No usable persistent memory is found");
    }
  }

  /**
   * The cache file is named as BlockPoolId-BlockId.
   */
  public String getCacheFileName(ExtendedBlockId key) {
    return key.getBlockPoolId() + "-" + key.getBlockId();
  }

  /**
   * Generate a file path to which the block replica will be mapped.
   * Considering the pmem volume size is below TB level currently,
   * it is tolerable to keep cache files under one directory.
   * This strategy will be optimized, especially if pmem has huge
   * cache capacity.
   */
  public String generateCacheFilePath(ExtendedBlockId key) throws IOException {
    return getOneLocation() + "/" + getCacheFileName(key);
  }
}
