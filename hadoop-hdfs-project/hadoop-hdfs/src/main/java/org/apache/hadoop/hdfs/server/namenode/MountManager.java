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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MountInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.BlockCacheManagerFactory;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.MountCacheManagerSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MOUNT_TMP_DIR;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MOUNT_TMP_DIR_DEFAULT;

/**
 * This class manages the mount path for remote stores. It keeps
 * track of the mounts that have completed along with those that are in the
 * process of being created.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MountManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(MountManager.class);

  public static final String IS_MOUNT_XATTR_NAME = "isMount";

  /**
   * Map from mount paths, that are being created, to ProvidedVolumeInfo
   * of the remote stores being mounted.
   */
  private final Map<Path, ProvidedVolumeInfo> mountsUnderCreation =
      new HashMap<>();

  /**
   * Map from mount paths to ProvidedVolumeInfo of the remote stores mounted.
   */
  private final Map<Path, ProvidedVolumeInfo> mountPoints = new HashMap<>();

  /**
   * Base temporary location used for creation of mounts.
   */
  private final String rootTempDirectory;

  private final FSNamesystem namesystem;
  private final Pattern mountTempDirectoryPattern;

  private MountCacheManagerSpi cacheManager;

  /**
   * A factory method to create {@link MountManager} on need basis. If
   * Provided storage is disabled, a no-op instance is created to avoid any
   * interference.
   * @param conf
   * @param namesystem
   * @param blockManager
   * @return
   */
  public static MountManager createInstance(Configuration conf,
      FSNamesystem namesystem, BlockManager blockManager) {
    ProvidedStorageMap providedStorageMap =
        namesystem.getBlockManager().getProvidedStorageMap();
    if (providedStorageMap.isProvidedEnabled()) {
      return new MountManager(conf, namesystem, blockManager);
    } else {
      LOG.info("Provided storage is not enabled, disable mount manager");
      return new NoOpMountManager(namesystem);
    }
  }

  private MountManager(FSNamesystem fsn) {
    rootTempDirectory = null;
    mountTempDirectoryPattern = null;
    namesystem = fsn;
  }

  private MountManager(Configuration conf, FSNamesystem namesystem,
      BlockManager blockManager) {
    String tempDir = conf.get(DFS_NAMENODE_MOUNT_TMP_DIR,
        DFS_NAMENODE_MOUNT_TMP_DIR_DEFAULT);
    if (!new Path(tempDir).isAbsolute()) {
      throw new IllegalArgumentException(
          DFS_NAMENODE_MOUNT_TMP_DIR + " is expected to be absolute");
    }
    if (tempDir.endsWith("/")) {
      tempDir = tempDir.substring(0, tempDir.length()-1);
    }
    this.rootTempDirectory = tempDir;
    this.namesystem = namesystem;
    // the path should be of the form TEMPORARY_DIR/VOL_UUID/<mount path>/..
    this.mountTempDirectoryPattern =
        Pattern.compile(String.format("%s/([^/]+)(/.+)", rootTempDirectory));

    this.cacheManager = BlockCacheManagerFactory.getFactory(conf)
        .newInstance(conf, namesystem, blockManager);
  }

  /**
   * @return the root path where mounts are created before being
   *         promoted to the final path.
   */
  String getRootTemporaryDir() {
    return rootTempDirectory;
  }

  /**
   * Start tracking a new mount path.
   *
   * @param mountPath mount path.
   * @param providedVolumeInfo {@link ProvidedVolumeInfo} of this mount.
   * @return the temporary path where the mount should be created.
   * @throws IOException if the mount path being added already exists.
   */
  synchronized String startMount(Path mountPath,
      ProvidedVolumeInfo providedVolumeInfo) throws IOException {
    if (!mountPath.isAbsolute()) {
      throw new IllegalArgumentException(
          "Mount path " + mountPath + " is not absolute!");
    }
    if (mountExists(mountPath)) {
      throw new IOException("Mount path " + mountPath + " already exists");
    }
    LOG.info("Starting mount path {} for remote path {}",
        mountPath, providedVolumeInfo.getRemotePath());
    mountsUnderCreation.put(mountPath, providedVolumeInfo);
    return getTemporaryPathForMount(mountPath, providedVolumeInfo);
  }

  /**
   * Finish the creation of a mount. This should be called after a
   * mount path has been fully created.
   *
   * @param mountPath the mount path.
   * @throws IOException if mount path is not in the process of being created.
   */
  synchronized void finishMount(Path mountPath) throws IOException {
    if (!mountsUnderCreation.containsKey(mountPath)) {
      throw new IOException("Mount path " + mountPath +
          " is not being created");
    }
    if (mountPoints.containsKey(mountPath)) {
      throw new IOException("Mount path " + mountPath +
          " has been completed");
    }
    LOG.info("Finishing mount {}.", mountPath);
    ProvidedVolumeInfo volInfo = mountsUnderCreation.remove(mountPath);
    mountPoints.put(mountPath, volInfo);
    // add this to the cacheManager.
    cacheManager.addMount(mountPath, volInfo);
  }

  /**
   * Check if the mount path already exists!
   *
   * @param mountPath the mount path.
   * @return true if the mount path exists or is being created.
   */
  synchronized boolean mountExists(Path mountPath) {
    return mountPoints.containsKey(mountPath) ||
        mountsUnderCreation.containsKey(mountPath);
  }

  /**
   * Remove the mount path from the list of mount paths maintained.
   *
   * @param mountPath the mount path.
   * @return Info of the removed volume.
   * @throws IOException if mount path was not finished.
   */
  synchronized ProvidedVolumeInfo removeMountPoint(Path mountPath)
      throws IOException {
    if (mountsUnderCreation.containsKey(mountPath)) {
      LOG.info("Removing mount under creation: {}", mountPath);
      ProvidedVolumeInfo info = mountsUnderCreation.remove(mountPath);
      return info;
    }
    if (!mountPoints.containsKey(mountPath)) {
      throw new IOException("Mount path " + mountPath +
          " does not exist");
    }
    LOG.info("Removing mount {}", mountPath);
    cacheManager.removeMount(mountPath);
    return mountPoints.remove(mountPath);
  }

  /**
   * @param mountPath mount path.
   * @param providedVolumeInfo {@link ProvidedVolumeInfo} of the mount.
   * @return the temporary location where the {@code mountPath} is created.
   */
  @VisibleForTesting
  String getTemporaryPathForMount(Path mountPath,
      ProvidedVolumeInfo providedVolumeInfo) {
    if (!mountPath.isAbsolute()) {
      throw new IllegalArgumentException(
          "Mount path " + mountPath + " is not absolute!");
    }
    String uuid = providedVolumeInfo.getId().toString();
    String relativePath = mountPath.toString().substring(1);
    return new Path(
        new Path(rootTempDirectory, uuid), relativePath).toString();
  }

  /**
   * Get the mount path whose temporary path includes the given path.
   *
   * @param path the path to check.
   * @return the corresponding mount path or null if none exists.
   */
  synchronized Path getMountFromTemporaryPath(String path) {
    Matcher matcher = mountTempDirectoryPattern.matcher(path);
    if (!matcher.matches()) {
      return null;
    }

    String uuid = matcher.group(1);
    Path remainingPath = new Path(matcher.group(2));
    Path mountPath = getMount(remainingPath, mountsUnderCreation);
    if (mountPath == null) {
      return null;
    }
    // check that the UUID in path matches the UUID in the mountinfo.
    UUID mountUUID = mountsUnderCreation.get(mountPath).getId();
    if (!mountUUID.toString().equals(uuid)) {
      LOG.error(
          "UUID of mounted path: {} doesn't match UUID of the mount: {}",
          uuid, mountUUID);
      return null;
    } else {
      return mountPath;
    }
  }

  /**
   * Get the mount path (completed or under construction) that includes the
   * given path.
   *
   * @param path the path to check.
   * @return the mount path that includes this path or null if none exists.
   */
  synchronized Path getMountPath(Path path) {
    if (path == null) {
      return null;
    }
    Path mount = getMount(path, mountPoints);
    return mount != null ? mount : getMount(path, mountsUnderCreation);
  }

  /**
   * Get the mount path that overlaps with the given path (one is a prefix of
   * the other).
   *
   * @param path
   * @return the mount path that overlaps with the given path or null if none
   *         exist.
   */
  synchronized Path getOverlappingMount(Path path) {
    if (path == null) {
      return null;
    }
    Path mount = getOverlappingMount(path, mountPoints);
    return mount != null ? mount
        : getOverlappingMount(path, mountsUnderCreation);
  }

  /**
   * Get the mount path that overlaps with the given path from the given mounts.
   *
   * @param path
   * @param mounts map of mount paths.
   * @return the mount path that overlaps with the given path or null if none
   *         exist.
   */
  private static Path getOverlappingMount(Path path,
      Map<Path, ProvidedVolumeInfo> mounts) {
    String pathStr = path.toString() + "/";
    for (Path mount : mounts.keySet()) {
      // add / at the end to make sure that the path is actually a prefix
      String mountStr = mount.toString() + "/";
      if (pathStr.startsWith(mountStr) || mountStr.startsWith(pathStr)) {
        return mount;
      }
    }
    return null;
  }

  /**
   * Get the mount path that includes the given path from the given mounts.
   *
   * @param path
   * @param mounts map of mount paths
   * @return the mount path that includes this path or null if none exists.
   */
  private static Path getMount(Path path,
      Map<Path, ProvidedVolumeInfo> mounts) {
    for (Path mount : mounts.keySet()) {
      // add / at the end to make sure that the path is actually a prefix
      String mountStr = mount.toString() + "/";
      String pathStr = path.toString() + "/";
      if (pathStr.startsWith(mountStr)) {
        return mount;
      }
    }
    return null;
  }

  synchronized int getNumberOfMounts() {
    return mountPoints.size() + mountsUnderCreation.size();
  }

  /**
   * @param mountPath mount path.
   * @return the {@link ProvidedVolumeInfo} associated with {@code mountPath}.
   */
  private synchronized ProvidedVolumeInfo getVolumeInfoForMount(Path mountPath) {
    ProvidedVolumeInfo info = mountPoints.get(mountPath);
    if (info == null) {
      info = mountsUnderCreation.get(mountPath);
    }
    return info;
  }

  /**
   * Get the existing mount paths or those being created.
   * @return map of mount to remote path.
   */
  @VisibleForTesting
  public synchronized Map<Path, ProvidedVolumeInfo> getMountPoints() {
    Map<Path, ProvidedVolumeInfo> allMounts = new HashMap<>();
    allMounts.putAll(mountPoints);
    allMounts.putAll(mountsUnderCreation);
    return allMounts;
  }

  /**
   * Get the finished mount paths.
   * @return map of mount to remote path.
   */
  synchronized Map<Path, ProvidedVolumeInfo> getFinishedMounts() {
    return Collections.unmodifiableMap(mountPoints);
  }

  /**
   * @return the map of unfinished mount paths.
   */
  synchronized Map<Path, ProvidedVolumeInfo> getUnfinishedMounts() {
    return Collections.unmodifiableMap(mountsUnderCreation);
  }

  /**
   * Clean up mounts if a failure occured during creation. This methods deletes
   * any intermediate files and state created during the mount process,
   * removes the mount path from the list of pending mounts and logs this
   * removal to the edit log. This method gets the namesystem write lock to
   * perform edit log operations.
   *
   * @param mountPath the mount path to remove.
   */
  void cleanUpMountOnFailure(Path mountPath) {
    cleanUpMountOnFailure(mountPath, true);
  }

  /**
   * Clean up mounts if a failure occured during creation. This methods deletes
   * any intermediate files and state created during the mount process, and
   * removes the mount path from the list of pending mounts. This method gets
   * the namesystem write lock to perform edit log operations.
   *
   * @param mountPath the mount path to remove.
   * @param logCleanup flag to log the clean up to the edit log.
   */
  void cleanUpMountOnFailure(Path mountPath, boolean logCleanup) {
    // we should get Namesystem lock before getting the lock on this object.
    namesystem.writeLock();
    try {
      synchronized (this) {
        ProvidedVolumeInfo mountInfo = mountsUnderCreation.get(mountPath);
        if (mountInfo == null) {
          LOG.warn("Cleanup called for mount ({}) not under construction.",
              mountPath);
          return;
        }
        String tempMountPath = getTemporaryPathForMount(mountPath, mountInfo);
        // delete the paths that were created
        try {
          if (namesystem.getFileInfo(tempMountPath, false, false, false) != null) {
            namesystem.deleteChecked(tempMountPath, true, false, path->{});
          }
        } catch (IOException e) {
          LOG.warn("Exception while cleaning up mount {}: ", mountPath, e);
        }
        mountsUnderCreation.remove(mountPath);
      }
      if (logCleanup) {
        namesystem.getEditLog().logRemoveMountOp(mountPath.toString());
      }
    } finally {
      namesystem.writeUnlock();
    }
    namesystem.getEditLog().logSync();
  }

  /**
   * Update the status of the mounts when the Namenode becomes active.
   * This includes cleanup of the mounts that were not completed at the previous
   * active Namenode. The namesystem write lock needs to be obtained before
   * calling this method.
   *
   * @throws IOException
   */
  synchronized void startService() throws IOException {
    assert namesystem.hasWriteLock();
    if (mountsUnderCreation.size() > 0) {
      // we have some mounts that haven't finished on the previous active.
      // create a copy of the keys to avoid ConcurrentModificationException.
      Set<Path> mountPaths = new HashSet<>(mountsUnderCreation.keySet());
      for (Path mount : mountPaths) {
        // if the mount path doesn't exist, clean it up
        String mountStr = mount.toString();
        if (namesystem.getFileInfo(mountStr, false, false, false) == null) {
          LOG.info("Calling cleanup for mount {} after failover", mount);
          cleanUpMountOnFailure(mount);
        } else {
          // check if the path is actually a mount path.
          List<XAttr> xAttrList = namesystem.listXAttrs(mountStr);
          boolean isMount = true;
          if (xAttrList.size() > 0) {
            for (XAttr xAttr : xAttrList) {
              if (xAttr.getName().equals(IS_MOUNT_XATTR_NAME) &&
                  xAttr.getNameSpace() == XAttr.NameSpace.USER) {
                isMount = true;
              }
            }
          }
          if (isMount) {
            LOG.info("Mount {} was successfully created before failover", mount);
            finishMount(mount);
          } else {
            LOG.warn("Mount {} was unable to finish successfully before " +
                "failover but the path exists; deleting form list of mounts " +
                "under construction", mount);
            cleanUpMountOnFailure(mount);
          }
        }
      }
    }

    cacheManager.startService();
    // delete whatever is under the temporary directory for the mounts.
    String tempMountDir = getRootTemporaryDir();
    if (!namesystem.isInSafeMode()
        && namesystem.getFileInfo(tempMountDir, false, false, false) != null) {
      namesystem.delete(tempMountDir, true, false);
    }
  }

  public void stopServices() {
    cacheManager.stopService();
  }

  @VisibleForTesting
  Pattern getMountTempDirectoryPattern() {
    return mountTempDirectoryPattern;
  }

  Path getRemotePathUnderMount(INodeFile file) {
    String localPathName = file.getName();
    Path mountPath = getMountFromTemporaryPath(localPathName);
    if (mountPath != null) {
      ProvidedVolumeInfo info = getVolumeInfoForMount(mountPath);
      Path baseRemotePath = new Path(info.getRemotePath());
      // change the prefix of the local name to the remote name.
      String tempPath = getTemporaryPathForMount(mountPath, info);
      String remotePath = localPathName
          .replace(tempPath, baseRemotePath.toString());
      return new Path(remotePath);
    }
    return null;
  }

  public void deleteBlocks(List<BlockInfo> toDeleteList)
      throws IOException {
    // TODO move all this to removeMount() --- shouldn't have two places for this.
    namesystem.getBlockManager().getProvidedStorageMap()
        .deleteBlocksFromAliasMap(toDeleteList);
    cacheManager.removeBlocks(toDeleteList);
  }

  /**
   * @return List of existing mounts.
   */
  public synchronized List<MountInfo> listMounts() {

    List<MountInfo> mounts = new ArrayList<>();
    for (ProvidedVolumeInfo info : mountPoints.values()) {
      mounts.add(new MountInfo(info.getMountPath(), info.getRemotePath(),
          MountInfo.MountStatus.CREATED));
    }

    for (ProvidedVolumeInfo info : mountsUnderCreation.values()) {
      mounts.add(new MountInfo(info.getMountPath(), info.getRemotePath(),
          MountInfo.MountStatus.CREATING));
    }

    return mounts;
  }

  public void addCachedBlock(BlockInfo storedBlock,
      DatanodeStorageInfo storageInfo) {
    cacheManager.addBlock(storedBlock, storageInfo);
  }

  public void removeCachedBlock(BlockInfo storedBlock,
      DatanodeDescriptor node) {
    cacheManager.removeCachedBlock(storedBlock, node);
  }

  MountCacheManagerSpi getMountCacheManager() {
    return cacheManager;
  }

  /**
   * If provided volumes are disabled, this delegate mount manager will
   * disable mount manager functionality by ignoring all requests
   */
  private static class NoOpMountManager extends MountManager {
    private NoOpMountManager(FSNamesystem namesystem) {
      super(namesystem);
      super.cacheManager = new MountCacheManagerSpi() {
      };
    }

    @Override
    public void addCachedBlock(BlockInfo storedBlock,
        DatanodeStorageInfo storageInfo) {
    }

    @Override
    public void removeCachedBlock(BlockInfo storedBlock,
        DatanodeDescriptor node) {
    }

    @Override
    Path getRemotePathUnderMount(INodeFile file) {
      return null;
    }

    @Override
    synchronized Path getMountFromTemporaryPath(String path) {
      return null;
    }

    @Override
    public void stopServices() {
    }

    @Override
    synchronized void startService() {
    }
  }
}
