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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_LOADER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_CACHE_PMEM_DIRS_KEY;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveIterator;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.TestFsDatasetCache;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;

import net.jcip.annotations.NotThreadSafe;

/**
 * Test HDFS cache using non-volatile storage class memory.
 */
@NotThreadSafe
public class TestCacheWithPmemMappableBlockLoader extends TestFsDatasetCache {
  private static final String PMEM_DIR =
      MiniDFSCluster.getBaseDirectory() + "/pmem";

  static {
    LogManager.getLogger(FsDatasetCache.class).setLevel(Level.DEBUG);
  }

  @Override
  protected void postSetupConf(Configuration config) {
    config.set(DFS_DATANODE_CACHE_PMEM_DIRS_KEY, PMEM_DIR);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    File pmem = new File(PMEM_DIR);
    pmem.delete();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    File pmem = new File(PMEM_DIR).getAbsoluteFile();
    pmem.mkdirs();
    try {
      PmemVolumeManager.verifyIfValidPmemVolume(new File(PMEM_DIR));
    } catch (Throwable t) {
      LogManager.getLogger(FsDatasetCache.class).warn(
          "Skip Pmem Cache test due to: " + t.getMessage());
    }
  }

  @Test
  public void testPmemConfiguration() throws Exception {
    shutdownCluster();

    String pmem0 = MiniDFSCluster.getBaseDirectory() + "/mnt/pmem0";
    String pmem1 = MiniDFSCluster.getBaseDirectory() + "/mnt/pmem1";
    new File(pmem0).getAbsoluteFile().mkdirs();
    new File(pmem1).getAbsoluteFile().mkdirs();
    try {
      PmemVolumeManager.verifyIfValidPmemVolume(new File(pmem0));
      PmemVolumeManager.verifyIfValidPmemVolume(new File(pmem1));
    } catch (Throwable t) {
      LogManager.getLogger(FsDatasetCache.class).warn(
          "Skip Pmem Cache test due to: " + t.getMessage());
      return;
    }

    Configuration myConf = new HdfsConfiguration();
    myConf.set(DFS_DATANODE_CACHE_LOADER_CLASS,
        "org.apache.hadoop.hdfs.server.datanode." +
            "fsdataset.impl.PmemMappableBlockLoader");
    // Set two persistent memory directories for HDFS cache
    myConf.set(DFS_DATANODE_CACHE_PMEM_DIRS_KEY, pmem0 + "," + pmem1);
    myConf.setLong(DFS_DATANODE_CACHE_PMEM_CAPACITY_KEY, CACHE_CAPACITY);

    MiniDFSCluster myCluster = new MiniDFSCluster.Builder(myConf)
        .numDataNodes(1).build();
    myCluster.waitActive();
    DataNode dataNode = myCluster.getDataNodes().get(0);
    MappableBlockLoader loader = ((FsDatasetImpl)dataNode.getFSDataset())
        .cacheManager.getMappableBlockLoader();
    assertTrue(loader instanceof PmemMappableBlockLoader);
    PmemVolumeManager pmemVolumeManager =
        ((PmemMappableBlockLoader) loader).getPmemVolumeManager();
    assertNotNull(pmemVolumeManager);
    // Test round-robin selection policy
    long count1 = 0, count2 = 0;
    for (int i = 0; i < 10; i++) {
      Byte index = pmemVolumeManager.getOneVolumeIndex();
      String volume = pmemVolumeManager.getVolumeByIndex(index);
      if (volume.equals(pmem0)) {
        count1++;
      } else if (volume.equals(pmem1)) {
        count2++;
      } else {
        fail("Unexpected persistent storage location:" + volume);
      }
    }
    assertEquals(count1, count2);
    myCluster.shutdown();
  }

  @Test(timeout=120000)
  public void testWaitForCachedReplicas() throws Exception {
    shutdownCluster();
    final int numDataNodes = 2;
    Configuration myConf = new HdfsConfiguration();
    myConf.set(DFS_DATANODE_CACHE_LOADER_CLASS,
        "org.apache.hadoop.hdfs.server.datanode." +
            "fsdataset.impl.PmemMappableBlockLoader");
    myConf.set(DFS_DATANODE_CACHE_PMEM_DIRS_KEY, PMEM_DIR);
    myConf.setLong(DFS_DATANODE_CACHE_PMEM_CAPACITY_KEY, CACHE_CAPACITY);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(myConf)
        .numDataNodes(numDataNodes).build();
    cluster.waitActive();

    FileSystemTestHelper helper = new FileSystemTestHelper();
    NameNode namenode = cluster.getNameNode();
    DistributedFileSystem dfs = cluster.getFileSystem();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return ((namenode.getNamesystem().getCacheCapacity() ==
            (numDataNodes * CACHE_CAPACITY)) &&
              (namenode.getNamesystem().getCacheUsed() == 0));
      }
    }, 500, 60000);

    // Send a cache report referring to a bogus block.  It is important that
    // the NameNode be robust against this.
    NamenodeProtocols nnRpc = namenode.getRpcServer();
    DataNode dn0 = cluster.getDataNodes().get(0);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    LinkedList<Long> bogusBlockIds = new LinkedList<Long>();
    bogusBlockIds.add(999999L);
    nnRpc.cacheReport(dn0.getDNRegistrationForBP(bpid), bpid, bogusBlockIds);

    Path rootDir = helper.getDefaultWorkingDirectory(dfs);
    // Create the pool
    final String pool = "friendlyPool";
    nnRpc.addCachePool(new CachePoolInfo("friendlyPool"));
    // Create some test files
    final int numFiles = 1;
    final int numBlocksPerFile = 2;
    final List<String> paths = new ArrayList<String>(numFiles);
    for (int i=0; i<numFiles; i++) {
      Path p = new Path(rootDir, "testCachePaths-" + i);
      FileSystemTestHelper.createFile(dfs, p, numBlocksPerFile,
          (int)BLOCK_SIZE);
      paths.add(p.toUri().getPath());
    }
    // Check the initial statistics at the namenode
    waitForCachedBlocks(namenode, 0, 0, "testWaitForCachedReplicas:0");

    // Cache and check each path in sequence
    int expected = 0;
    for (int i=0; i<numFiles; i++) {
      CacheDirectiveInfo directive =
          new CacheDirectiveInfo.Builder().
            setPath(new Path(paths.get(i))).
            setPool(pool).
            build();
      nnRpc.addCacheDirective(directive, EnumSet.noneOf(CacheFlag.class));
      expected += numBlocksPerFile;
      waitForCachedBlocks(namenode, expected, expected,
          "testWaitForCachedReplicas:1");
    }

    // Check that the datanodes have the right cache values
    DatanodeInfo[] live = dfs.getDataNodeStats(DatanodeReportType.LIVE);
    assertEquals("Unexpected number of live nodes", numDataNodes,
        live.length);
    long totalUsed = 0;
    for (DatanodeInfo dn : live) {
      final long cacheCapacity = dn.getCacheCapacity();
      final long cacheUsed = dn.getCacheUsed();
      final long cacheRemaining = dn.getCacheRemaining();
      assertEquals("Unexpected cache capacity", CACHE_CAPACITY, cacheCapacity);
      assertEquals("Capacity not equal to used + remaining",
          cacheCapacity, cacheUsed + cacheRemaining);
      assertEquals("Remaining not equal to capacity - used",
          cacheCapacity - cacheUsed, cacheRemaining);
      totalUsed += cacheUsed;
    }
    assertEquals(expected*BLOCK_SIZE, totalUsed);

    // Uncache and check each path in sequence
    RemoteIterator<CacheDirectiveEntry> entries =
        new CacheDirectiveIterator(nnRpc, null, FsTracer.get(myConf));
    for (int i=0; i<numFiles; i++) {
      CacheDirectiveEntry entry = entries.next();
      nnRpc.removeCacheDirective(entry.getInfo().getId());
      expected -= numBlocksPerFile;
      waitForCachedBlocks(namenode, expected, expected,
          "testWaitForCachedReplicas:2");
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout=120000)
  public void testWriteRead() throws Exception {
    shutdownCluster();
    final int numDataNodes = 2;
    Configuration myConf = new HdfsConfiguration();
    myConf.set(DFS_DATANODE_CACHE_LOADER_CLASS,
        "org.apache.hadoop.hdfs.server.datanode." +
            "fsdataset.impl.PmemMappableBlockLoader");
    myConf.set(DFS_DATANODE_CACHE_PMEM_DIRS_KEY, PMEM_DIR);
    myConf.setLong(DFS_DATANODE_CACHE_PMEM_CAPACITY_KEY, CACHE_CAPACITY);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(myConf)
        .numDataNodes(numDataNodes).build();
    cluster.waitActive();

    FileSystemTestHelper helper = new FileSystemTestHelper();
    NameNode namenode = cluster.getNameNode();
    DistributedFileSystem dfs = cluster.getFileSystem();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return ((namenode.getNamesystem().getCacheCapacity() ==
            (numDataNodes * CACHE_CAPACITY)) &&
              (namenode.getNamesystem().getCacheUsed() == 0));
      }
    }, 500, 60000);

    // Send a cache report referring to a bogus block.  It is important that
    // the NameNode be robust against this.
    NamenodeProtocols nnRpc = namenode.getRpcServer();
    DataNode dn0 = cluster.getDataNodes().get(0);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    LinkedList<Long> bogusBlockIds = new LinkedList<Long>();
    bogusBlockIds.add(999999L);
    nnRpc.cacheReport(dn0.getDNRegistrationForBP(bpid), bpid, bogusBlockIds);

    Path rootDir = helper.getDefaultWorkingDirectory(dfs);
    // Create the pool
    final String pool = "friendlyPool";
    nnRpc.addCachePool(new CachePoolInfo("friendlyPool"));
    // Create some test files
    final int numFiles = 1;
    final int numBlocksPerFile = 2;
    final List<String> paths = new ArrayList<String>(numFiles);
    for (int i=0; i<numFiles; i++) {
      Path p = new Path(rootDir, "testCachePaths-" + i);
      FileSystemTestHelper.createFile(dfs, p, numBlocksPerFile,
          (int)BLOCK_SIZE);
      paths.add(p.toUri().getPath());
    }
    // Check the initial statistics at the namenode
    waitForCachedBlocks(namenode, 0, 0, "testWriteRead:0");

    // Read File content
    int blkExpected = 0;
    Path fileRead = new Path(rootDir, "testCachePaths-" + 55);
    final byte[] data = FileSystemTestHelper.getFileData(3, BLOCK_SIZE);
    long fileLength = FileSystemTestHelper.createFile(dfs, fileRead, data,
        (int) BLOCK_SIZE, (short) 1);
    CacheDirectiveInfo directive = new CacheDirectiveInfo.Builder()
        .setPath(fileRead).setPool(pool).build();
    nnRpc.addCacheDirective(directive, EnumSet.noneOf(CacheFlag.class));
    blkExpected += 3;
    waitForCachedBlocks(namenode, blkExpected, blkExpected,
        "testWriteRead:1");
    // read file content
    FileStatus fileStatus = dfs.getFileStatus(fileRead);
    Assert.assertEquals(fileStatus.getLen(), fileLength);
    AppendTestUtil.checkFullFile(dfs, fileRead, (int) fileLength, data,
        fileRead.toString());

    // Uncache and check each path in sequence
    RemoteIterator<CacheDirectiveEntry> entries =
        new CacheDirectiveIterator(nnRpc, null, FsTracer.get(myConf));
    for (int i = 0; i < numFiles; i++) {
      CacheDirectiveEntry entry = entries.next();
      nnRpc.removeCacheDirective(entry.getInfo().getId());
      waitForCachedBlocks(namenode, 0, 0, "testWriteRead:2");
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
