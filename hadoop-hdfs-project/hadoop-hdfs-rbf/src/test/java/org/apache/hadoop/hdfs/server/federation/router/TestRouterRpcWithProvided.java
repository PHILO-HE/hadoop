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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MountInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttr.NameSpace;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MockResolver;
import org.apache.hadoop.hdfs.util.RemoteMountUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PROVIDED_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRouterRpcWithProvided {

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** Random Router for this federated cluster. */
  private static MiniRouterDFSCluster.RouterContext router;

  /** Client interface to the Router. */
  private static ClientProtocol routerProtocol;

  static private Configuration conf;

  private static String ns0;

  private final static String ns0RBFMount = "/mnt/";

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniRouterDFSCluster(false, 2);
    cluster.setNumDatanodesPerNameservice(3);

    MiniDFSNNTopology topo = cluster.buildAndGetTopology();
    MiniDFSCluster.setupNamenodeProvidedConfiguration(conf, topo);

    TestRouterRpc.startRouterDFSCluster(cluster, conf, topo);
    // Random router for this test
    MiniRouterDFSCluster.RouterContext rndRouter = cluster.getRandomRouter();
    router = rndRouter;
    routerProtocol = rndRouter.getClient().getNamenode();

    MockResolver resolver =
        (MockResolver) router.getRouter().getSubclusterResolver();
    ns0 = cluster.getNameservices().get(0);
    resolver.addLocation(ns0RBFMount, ns0, "/");
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAddRemoveMount() throws Exception {
    // create new paths on ns1 to mount
    MiniDFSCluster dfsCluster = cluster.getCluster();
    String ns1 = cluster.getNameservices().get(1);
    FileSystem ns1FS =
        dfsCluster.getFileSystem(dfsCluster.getNNIndexes(ns1).get(0));
    FsPermission permission = new FsPermission("777");
    String path1 = "/folder0/test1/";
    ns1FS.mkdirs(new Path(path1), permission);
    String remotePath = ns1FS.getUri().toString() + "/";
    String providedMountPath = ns0RBFMount + "remotes";
    try {
      routerProtocol.addMount(remotePath, providedMountPath, null);
      fail("addMount cannot succeed without PROVIDED DNs");
    } catch(RemoteException re) {
      GenericTestUtils.assertExceptionContains(
          "Mount failed! Datanodes with PROVIDED storage unavailable." , re);
    }

    int lastDNIndex = dfsCluster.getDataNodes().size();
    // add a provided datanode to ns0
    StorageType[] storages =
        new StorageType[] {StorageType.DISK};
    Configuration newDNConf = new Configuration(dfsCluster.getConfiguration(0));
    newDNConf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, true);
    dfsCluster.startDataNodes(newDNConf, 1,
        new StorageType[][] {storages}, true,
        HdfsServerConstants.StartupOption.REGULAR,
        null, null, null, null, false, false, false, null);
    dfsCluster.waitActive();
    dfsCluster.waitFirstBRCompleted(0, 1000);
    // mount should now succeed!
    assertTrue(routerProtocol.addMount(remotePath, providedMountPath,
        RemoteMountUtils.decodeConfig("a=b,c=d")));
    HdfsFileStatus status = routerProtocol.getFileInfo(
        providedMountPath + path1);
    assertNotNull(status);
    // expect 0 child as none on remote.
    assertEquals(0, status.getChildrenNum());
    // check that the mount has been created correctly in ns0
    int providedNNIndex = dfsCluster.getNNIndexes(ns0).get(0);
    FileSystem ns0FS = dfsCluster.getFileSystem(providedNNIndex);
    assertTrue(ns0FS.exists(new Path("/remotes/")));

    List<XAttr> xAttrs = routerProtocol.getXAttrs(providedMountPath, null);
    assertEquals(3, xAttrs.size());
    verifyXAttrExists(NameSpace.USER, "true", "isMount", xAttrs);
    verifyXAttrExists(NameSpace.TRUSTED, "b", "mount.config.a", xAttrs);
    verifyXAttrExists(NameSpace.TRUSTED, "d", "mount.config.c", xAttrs);

    List<MountInfo> mountInfos = routerProtocol.listMounts();
    assertEquals("We wanted 1 mount point but got: " + mountInfos,
        1, mountInfos.size());
    assertEquals(providedMountPath, mountInfos.get(0).getMountPath());

    // unmount now and check that the folder is gone.
    assertTrue(routerProtocol.removeMount(providedMountPath));
    assertNull(routerProtocol.getFileInfo(
        providedMountPath + path1));
    assertFalse(ns0FS.exists(new Path("/remotes/")));

    // mount should succeed with null remote config too
    assertTrue(routerProtocol.addMount(remotePath, providedMountPath, null));
    status = routerProtocol.getFileInfo(providedMountPath + path1);
    assertNotNull(status);
    // expect 0 child as none on remote.
    assertEquals(0, status.getChildrenNum());
    // check that the mount has been created correctly in ns0
    assertTrue(ns0FS.exists(new Path("/remotes/")));

    xAttrs = routerProtocol.getXAttrs(providedMountPath, null);
    assertEquals(1, xAttrs.size());
    verifyXAttrExists(NameSpace.USER, "true", "isMount", xAttrs);

    // unmount now and check that the folder is gone.
    assertTrue(routerProtocol.removeMount(providedMountPath));
    assertNull(routerProtocol.getFileInfo(
        providedMountPath + path1));
    assertFalse(ns0FS.exists(new Path("/remotes/")));

    mountInfos = routerProtocol.listMounts();
    assertTrue(mountInfos.isEmpty());

    // remove the Provided datanode added above.
    DataNode providedDN = dfsCluster.getDataNodes().get(lastDNIndex);
    dfsCluster.shutdownDataNode(lastDNIndex);
    BlockManagerTestUtil.noticeDeadDatanode(
        dfsCluster.getNameNode(providedNNIndex),
        providedDN.getDatanodeId().getXferAddr());
  }

  private void verifyXAttrExists(NameSpace nameSpace, String value, String name,
      List<XAttr> xAttrs) {
    for (XAttr xAttr : xAttrs) {
      if (xAttr.getNameSpace().equals(nameSpace) && xAttr.getName().equals(name)
          && value.equals(new String(xAttr.getValue()))) {
        return;
      }
    }

    StringBuilder sb = new StringBuilder();
    for (XAttr xAttr : xAttrs) {
      sb.append(xAttr.getNameSpace()).append(":");
      sb.append(xAttr.getName()).append(":");
      sb.append(new String(xAttr.getValue())).append(" | ");
    }
    String msg = String.format("Missing xattr: %s:%s:%s in %s",
        nameSpace, name, value, sb.toString());
    fail(msg);
  }

  @Test
  public void testAddMountFailure() throws IOException {
    try {
      routerProtocol.addMount("hdfs://remote/hdfs/path", "/mnt/newmount", null);
      fail("AddMount cannot succeed without PROVIDED DNs");
    } catch(RemoteException re) {
      GenericTestUtils.assertExceptionContains(
          "Mount failed! Datanodes with PROVIDED storage unavailable." , re);
    }
  }

  @Test
  public void testRemoveMountFailure() throws IOException {
    String removeMountPath = "/mnt/path/to/mount";
    try {
      routerProtocol.removeMount(removeMountPath);
      fail("RemoveMount should fail as path doesn't exist.");
    } catch(RemoteException re) {
      GenericTestUtils.assertExceptionContains(
          "Mount path /path/to/mount does not exist" , re);
    }
  }
}
