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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.rocksdb.RocksDBException;

    // HA（高可用）服务接口：定义主从复制和故障切换的契约
    public interface HAService {

    // 初始化HA服务，必须在其他方法之前调用
    void init(DefaultMessageStore defaultMessageStore) throws IOException;

    // 启动HA服务
    void start() throws Exception;

    // 关闭HA服务
    void shutdown();

    // 切换到Master状态（Controller模式下使用）
    default boolean changeToMaster(int masterEpoch) throws RocksDBException {
        return false;
    }

    // 当上次角色已经是Master时，切换到Master状态
    default boolean changeToMasterWhenLastRoleIsMaster(int masterEpoch) {
        return false;
    }

    // 切换到Slave状态（Controller模式下使用）
    default boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        return false;
    }

    // 当Master未变化时，切换到Slave状态
    default boolean changeToSlaveWhenMasterNotChange(String newMasterAddr, int newMasterEpoch) {
        return false;
    }

    // 更新Master地址
    void updateMasterAddress(String newAddr);

    // 更新HA Master地址
     */
    void updateHaMasterAddress(String newAddr);

    /**
     * Returns the number of replicas those commit log are not far behind the master. It includes master itself. Returns
     * syncStateSet size if HAService instanceof AutoSwitchService
     *
     * @return the number of slaves
     * @see MessageStoreConfig#getHaMaxGapNotInSync()
     */
    int inSyncReplicasNums(long masterPutWhere);

    /**
     * Get connection count
     *
     * @return the number of connection
     */
    AtomicInteger getConnectionCount();

    /**
     * Put request to handle HA
     *
     * @param request
     */
    void putRequest(final CommitLog.GroupCommitRequest request);

    /**
     * Put GroupConnectionStateRequest for preOnline
     *
     * @param request
     */
    void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request);

    /**
     * Get ha connection list
     *
     * @return List<HAConnection>
     */
    List<HAConnection> getConnectionList();

    /**
     * Get HAClient
     *
     * @return HAClient
     */
    HAClient getHAClient();

    /**
     * Get the max offset in all slaves
     */
    AtomicLong getPush2SlaveMaxOffset();

    /**
     * Get HA runtime info
     */
    HARuntimeInfo getRuntimeInfo(final long masterPutWhere);

    /**
     * Get WaitNotifyObject
     */
    WaitNotifyObject getWaitNotifyObject();

    /**
     * Judge whether the slave keeps up according to the masterPutWhere, If the offset gap exceeds haSlaveFallBehindMax,
     * then slave is not OK
     */
    boolean isSlaveOK(long masterPutWhere);
}
