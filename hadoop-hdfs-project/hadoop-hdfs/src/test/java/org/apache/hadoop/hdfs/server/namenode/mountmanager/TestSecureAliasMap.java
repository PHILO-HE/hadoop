/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.MountInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.TestSecureNNWithQJM;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test DN-NN communication in secured hdfs and alias map
 */
public class TestSecureAliasMap {
  private static HdfsConfiguration baseConf;
  private static File baseDir;
  private static MiniKdc kdc;

  private static String keystoresDir;
  private static String sslConfDir;
  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;
  private NameNode nn;
  private FileSystem fs;
  private DFSClient client;

  @BeforeClass
  public static void init() throws Exception {
    baseDir =
        GenericTestUtils.getTestDir(TestSecureAliasMap.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    baseConf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, baseConf);
    UserGroupInformation.setConfiguration(baseConf);
    assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytabFile = new File(baseDir, userName + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    kdc.createPrincipal(keytabFile, userName + "/" + krbInstance,
        "HTTP/" + krbInstance);
    String hdfsPrincipal = userName + "/" + krbInstance + "@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();

    baseConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    baseConf
        .set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    baseConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    baseConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");

    baseConf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    baseConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSecureNNWithQJM.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
    baseConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    baseConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSecureNNWithQJM.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
    baseConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    baseConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @AfterClass
  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @After
  public void shutdown() throws IOException {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testSecureConnectionToAliasMap() throws Exception {
    conf = new HdfsConfiguration(baseConf);
    MiniDFSCluster.setupNamenodeProvidedConfiguration(conf);

    int numNodes = 1;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numNodes).storageTypes(
        new StorageType[] { StorageType.DISK, StorageType.PROVIDED }).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    nn = cluster.getNameNode(0);
    client = new DFSClient(nn.getNameNodeAddress(), conf);

    Path child = new Path(UUID.randomUUID().toString());
    String sourcePath = "/source";
    Path testFile = new Path(sourcePath, child);
    OutputStream out = cluster.getFileSystem().create(testFile);
    for (int i = 0; i < 10; ++i) {
      out.write(UUID.randomUUID().toString().getBytes());
    }
    out.close();

    FSNamesystem namesystem = cluster.getNamesystem();
    BlockManager blockManager = namesystem.getBlockManager();
    DataNode dn = cluster.getDataNodes().get(0);

    FsDatasetSpi.FsVolumeReferences volumes =
        dn.getFSDataset().getFsVolumeReferences();
    FsVolumeSpi providedVolume = null;
    for (FsVolumeSpi volume : volumes) {
      if (volume.getStorageType().equals(StorageType.PROVIDED)) {
        providedVolume = volume;
        break;
      }
    }

    String[] bps = providedVolume.getBlockPoolList();
    assertEquals(1, bps.length);

    BlockAliasMap aliasMap = blockManager.getProvidedStorageMap().getAliasMap();
    BlockAliasMap.Reader reader = aliasMap.getReader(null, bps[0]);
    assertNotNull(reader);

    URI clusterURI = cluster.getURI(0);
    String qualifiedRemotePath =
        new Path("/source").makeQualified(clusterURI, null).toString();
    client.addMount(qualifiedRemotePath, "/mount", null);

    List<MountInfo> mounts = client.listMounts();
    assertEquals(1, mounts.size());
  }
}