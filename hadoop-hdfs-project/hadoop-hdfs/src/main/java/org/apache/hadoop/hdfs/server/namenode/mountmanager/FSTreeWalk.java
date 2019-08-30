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
package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MOUNT_ACLS_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MOUNT_ACLS_ENABLED_DEFAULT;

/**
 * Traversal of an external FileSystem.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FSTreeWalk extends TreeWalk {

  public static final Logger LOG =
      LoggerFactory.getLogger(FSTreeWalk.class);

  private final Path root;
  private final FileSystem fs;
  private final boolean enableACLs;

  public FSTreeWalk(String root, Configuration conf) throws IOException {
    this(new Path(root), conf);
  }

  public FSTreeWalk(Path root, Configuration conf) throws IOException {
    this.root = root;
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    try {
      fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws IOException {
          return FileSystem.get(root.toUri(), conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    boolean mountACLsEnabled = conf.getBoolean(DFS_NAMENODE_MOUNT_ACLS_ENABLED,
        DFS_NAMENODE_MOUNT_ACLS_ENABLED_DEFAULT);
    boolean localACLsEnabled = conf.getBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    if (!localACLsEnabled && mountACLsEnabled) {
      LOG.warn("Mount ACLs have been enabled but HDFS ACLs are not. " +
          "Disabling ACLs on the mount {}", root);
      this.enableACLs = false;
    } else {
      this.enableACLs = mountACLsEnabled;
    }
  }

  @Override
  protected Iterable<TreePath> getChildren(TreePath path, long id,
      TreeIterator i) {
    // TODO symlinks
    if (!path.getFileStatus().isDirectory()) {
      return Collections.emptyList();
    }
    try {
      ArrayList<TreePath> ret = new ArrayList<>();
      for (FileStatus s : fs.listStatus(path.getFileStatus().getPath())) {
        ret.add(new TreePath(s, id, i, fs, path.getLocalMountPath(),
            path.getRemoteRoot(), getAclStatus(fs, s.getPath())));
      }
      return ret;
    } catch (FileNotFoundException e) {
      throw new ConcurrentModificationException("FS modified");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  class FSTreeIterator extends TreeIterator {

    private FSTreeIterator() {
    }

    FSTreeIterator(TreePath p) {
      AclStatus acls = null;
      if (enableACLs) {
        Path remotePath = p.getFileStatus().getPath();
        try {
          acls = fs.getAclStatus(remotePath);
        } catch (IOException e) {
          LOG.warn(
              "Got exception when trying to get remote acls for path {} : {}",
              remotePath, e.getMessage());
        }
      }
      getPendingQueue().addFirst(
          new TreePath(p.getFileStatus(), p.getParentId(), this, fs, null,
              root, acls));
    }

    FSTreeIterator(Path p) throws IOException {
      try {
        FileStatus s = fs.getFileStatus(root);
        getPendingQueue().addFirst(new TreePath(s, -1L, this, fs, null, root,
            getAclStatus(fs, s.getPath())));
      } catch (FileNotFoundException e) {
        if (p.equals(root)) {
          throw e;
        }
        throw new ConcurrentModificationException("FS modified");
      }
    }

    @Override
    public TreeIterator fork() {
      if (getPendingQueue().isEmpty()) {
        return new FSTreeIterator();
      }
      return new FSTreeIterator(getPendingQueue().removeFirst());
    }

  }

  private AclStatus getAclStatus(FileSystem fs, Path path) throws IOException {
    if (enableACLs) {
      try {
        return fs.getAclStatus(path);
      } catch (UnsupportedOperationException e) {
        LOG.warn("Remote filesystem {} doesn't support ACLs", fs);
      }
    }
    return null;
  }

  @Override
  public TreeIterator iterator() {
    try {
      return new FSTreeIterator(root);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path getRoot() {
    return root;
  }

}
