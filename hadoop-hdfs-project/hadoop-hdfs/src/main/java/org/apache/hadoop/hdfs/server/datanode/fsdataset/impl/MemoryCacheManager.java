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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Manages for memory caching.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryCacheManager implements MappableBlockClassLoader {
  public static final String MEMORY_MAPPED_BLOCK_CLASS =
      "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemoryMappedBlock";
  private Class clazz;

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
      clazz = loader.loadClass(MEMORY_MAPPED_BLOCK_CLASS);
      return clazz;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
