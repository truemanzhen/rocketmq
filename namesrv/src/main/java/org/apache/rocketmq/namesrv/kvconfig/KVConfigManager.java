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
package org.apache.rocketmq.namesrv.kvconfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.body.KVTable;

public class KVConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvController namesrvController;

    // 读写锁，保证KV配置表的并发安全
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // KV配置表：外层key为命名空间，内层key为配置项名，value为配置值
    private final HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable =
        new HashMap<>();

    public KVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    // 从磁盘文件加载KV配置到内存
    public void load() {
        String content = null;
        try {
            // 从配置文件路径读取JSON内容
            content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        } catch (IOException e) {
            log.warn("Load KV config table exception", e);
        }
        if (content != null) {
            // 反序列化JSON为KV配置对象
            KVConfigSerializeWrapper kvConfigSerializeWrapper =
                KVConfigSerializeWrapper.fromJson(content, KVConfigSerializeWrapper.class);
            if (null != kvConfigSerializeWrapper) {
                // 将文件中的配置合并到内存配置表
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load KV config table OK");
            }
        }
    }

    // 写入KV配置并持久化
    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                // 获取命名空间对应的KV表，不存在则创建
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null == kvTable) {
                    kvTable = new HashMap<>();
                    this.configTable.put(namespace, kvTable);
                    log.info("putKVConfig create new Namespace {}", namespace);
                }

                // 写入KV键值对，返回旧值
                final String prev = kvTable.put(key, value);
                if (null != prev) {
                    log.info("putKVConfig update config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                } else {
                    log.info("putKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putKVConfig InterruptedException", e);
        }

        // 写入后持久化到磁盘
        this.persist();
    }

    // 将内存中的KV配置持久化到磁盘文件
    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
                kvConfigSerializeWrapper.setConfigTable(this.configTable);

                // 序列化为JSON字符串
                String content = kvConfigSerializeWrapper.toJson();

                if (null != content) {
                    // 写入配置文件
                    MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
                }
            } catch (IOException e) {
                log.error("persist kvconfig Exception, "
                    + this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist InterruptedException", e);
        }

    }

    // 删除KV配置并持久化
    public void deleteKVConfig(final String namespace, final String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    // 从KV表中删除指定key
                    String value = kvTable.remove(key);
                    log.info("deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("deleteKVConfig InterruptedException", e);
        }

        this.persist();
    }

    // 获取指定命名空间下的所有KV配置
    public byte[] getKVListByNamespace(final String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    // 将KV表封装为KVTable对象并编码为字节数组
                    KVTable table = new KVTable();
                    table.setTable(kvTable);
                    return table.encode();
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVListByNamespace InterruptedException", e);
        }

        return null;
    }

    // 根据命名空间和key查询配置值
    public String getKVConfig(final String namespace, final String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    // 从KV表中获取对应的value
                    return kvTable.get(key);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVConfig InterruptedException", e);
        }

        return null;
    }

    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------------------------------------");

                {
                    log.info("configTable SIZE: {}", this.configTable.size());
                    for (Entry<String, HashMap<String, String>> next : this.configTable.entrySet()) {
                        for (Entry<String, String> nextSub : next.getValue().entrySet()) {
                            log.info("configTable NS: {} Key: {} Value: {}", next.getKey(), nextSub.getKey(),
                                    nextSub.getValue());
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("printAllPeriodically InterruptedException", e);
        }
    }
}
