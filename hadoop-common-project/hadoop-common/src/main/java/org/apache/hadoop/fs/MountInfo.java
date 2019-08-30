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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

import java.util.Objects;

/**
 * Represents the information for a mount point / provided volume.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MountInfo {

  /**
   * The status of the mount.
   */
  public enum MountStatus {
    // TODO add other statuses
    CREATING, CREATED;
  }

  // The local HDFS path where remote file system is mounted.
  private final String mountPath;

  // The path on remote file system.
  private final String remotePath;

  // The status of the mount.
  private final MountStatus status;

  /**
   * Constructor
   * @param mountPath the local hdfs path where remote file system is mounted.
   * @param remotePath the path on remote file system.
   * @param status the status of the mount.
   */
  public MountInfo(String mountPath, String remotePath, MountStatus status) {
    this.mountPath = mountPath;
    this.remotePath = remotePath;
    this.status = status;
  }

  /**
   * Constructor.
   * @param rbfMount the RBF mount path (appended to the mount path in
   *                 {@code info} as a prefix.
   * @param info {@link MountInfo} from the sub-clusters.
   */
  public MountInfo(String rbfMount, MountInfo info) {
    this.mountPath = rbfMount + info.mountPath;
    this.remotePath = info.remotePath;
    this.status = info.status;
  }

  /**
   * @return The local HDFS path where remote file system is mounted.
   */
  public String getMountPath() {
    return mountPath;
  }

  /**
   * @return The path on remote file system.
   */
  public String getRemotePath() {
    return remotePath;
  }

  /**
   * @return the mount status.
   */
  public MountStatus getMountStatus() {
    return status;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MountInfo)) {
      return false;
    }
    MountInfo other = (MountInfo) obj;
    return this.mountPath.equals(other.getMountPath())
        && this.remotePath.equals(other.getRemotePath())
        && this.status.equals(other.getMountStatus());
  }

  @Override
  public String toString() {
    return "MountPath: " + mountPath + ", RemotePath: " + remotePath
        + ", Status: " + status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mountPath, remotePath, status);
  }

  public static MountStatus parseMountStatus(String s) {
    return MountStatus.valueOf(StringUtils.toUpperCase(s));
  }
}
