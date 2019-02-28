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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Manage the non-volatile storage class memory cache volumes.
 *
 * TODO: Refine persistent location considering storage utilization
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemCacheManager implements MappableBlockClassLoader {
  public static final String PMEM_MAPPED_BLOCK_CLASS =
      "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.PmemMappedBlock";
  private static final Logger LOG = LoggerFactory.getLogger(PmemCacheManager
      .class);
  private final ArrayList<String> pmemVolumes = new ArrayList<String>();
  // It's not a strict atomic operation for the performance sake.
  private int index = 0;
  private int count = 0;
  private Class clazz;

  public PmemCacheManager(String[] pmemVolumes) throws IOException {
    PmemMappedBlock.setPersistentMemoryManager(this);
    this.load(pmemVolumes);
  }

  @Override
  public Class loadMappableBlockClass() {
    if (clazz != null) {
      return clazz;
    }

    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if (loader == null) {
        loader = ClassLoader.getSystemClassLoader();
      }
      clazz = loader.loadClass(PMEM_MAPPED_BLOCK_CLASS);
      return clazz;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public void load(String[] volumes) throws IOException {
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
}
