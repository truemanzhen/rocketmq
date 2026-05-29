/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.config;

import java.io.File;

    // 存储路径配置助手：提供各种存储文件的路径
    public class StorePathConfigHelper {

    // 获取ConsumeQueue存储路径
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }

    // 获取ConsumeQueue扩展存储路径
    public static String getStorePathConsumeQueueExt(final String rootDir) {
        return rootDir + File.separator + "consumequeue_ext";
    }

    // 获取批量ConsumeQueue存储路径
    public static String getStorePathBatchConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "batchconsumequeue";
    }

    // 获取RocksDB ConsumeQueue存储路径
    public static String getStorePathRocksDBConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue_rocksdb";
    }

    // 获取索引文件存储路径
    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }

    // 获取检查点文件路径
    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }

    // 获取abort文件路径
    public static String getAbortFile(final String rootDir) {
        return rootDir + File.separator + "abort";
    }

    // 获取锁文件路径
    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }

    // 获取延迟偏移量存储路径
    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }

    // 获取事务状态表存储路径
    public static String getTranStateTableStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "statetable";
    }

    // 获取事务重做日志存储路径
    public static String getTranRedoLogStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "redolog";
    }

}
