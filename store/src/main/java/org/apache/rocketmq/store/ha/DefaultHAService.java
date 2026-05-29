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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

    // 默认HA服务实现：基于NIO的主从复制
    public class DefaultHAService implements HAService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 当前连接数
    protected final AtomicInteger connectionCount = new AtomicInteger(0);

    // HA连接列表
    protected final List<HAConnection> connectionList = new LinkedList<>();

    // 接受Socket连接的服务
    protected AcceptSocketService acceptSocketService;

    protected DefaultMessageStore defaultMessageStore;

    // 等待通知对象（用于同步刷盘）
    protected WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    // 推送到Slave的最大偏移量
    protected AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    // 组传输服务（同步复制时使用）
    protected GroupTransferService groupTransferService;

    // HA客户端（Slave端使用，连接Master拉取数据）
    protected HAClient haClient;

    // HA连接状态通知服务
    protected HAConnectionStateNotificationService haConnectionStateNotificationService;

    public DefaultHAService() {
    }

    @Override
    // 初始化HA服务
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService = new DefaultAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
        // 同步复制传输服务
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        // Slave端创建HA客户端，连接Master拉取数据
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            this.haClient = new DefaultHAClient(this.defaultMessageStore);
        }
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    // 更新Master地址
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    @Override
    // 更新HA Master地址
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    // 提交同步复制请求
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    @Override
    // 检查Slave是否正常
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && masterPutWhere - this.push2SlaveMaxOffset.get() < this.defaultMessageStore
                .getMessageStoreConfig().getHaMaxGapNotInSync();
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haConnectionStateNotificationService.start();
        if (haClient != null) {
            this.haClient.start();
        }
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        this.haConnectionStateNotificationService.checkConnectionStateAndNotify(conn);
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        if (this.acceptSocketService != null) {
            this.acceptSocketService.shutdown(true);
        }
        this.destroyConnections();
        if (this.groupTransferService != null) {
            groupTransferService.shutdown();
        }

        if (this.haConnectionStateNotificationService != null) {
            this.haConnectionStateNotificationService.shutdown();
        }
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        int inSyncNums = 1;
        for (HAConnection conn : this.connectionList) {
            if (this.isInSyncSlave(masterPutWhere, conn)) {
                inSyncNums++;
            }
        }
        return inSyncNums;
    }

    protected boolean isInSyncSlave(final long masterPutWhere, HAConnection conn) {
        if (masterPutWhere - conn.getSlaveAckOffset() < this.defaultMessageStore.getMessageStoreConfig()
            .getHaMaxGapNotInSync()) {
            return true;
        }
        return false;
    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
        this.haConnectionStateNotificationService.setRequest(request);
    }

    @Override
    public List<HAConnection> getConnectionList() {
        return connectionList;
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();

        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);

            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);
            int inSyncNums = 0;

            info.setMasterCommitLogMaxOffset(masterPutWhere);

            for (HAConnection conn : this.connectionList) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

                long slaveAckOffset = conn.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(conn.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(conn.getTransferFromWhere());

                boolean isInSync = this.isInSyncSlave(masterPutWhere, conn);
                if (isInSync) {
                    inSyncNums++;
                }
                cInfo.setInSync(isInSync);

                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(inSyncNums);
        }
        return info;
    }

    class DefaultAcceptSocketService extends AcceptSocketService {

        public DefaultAcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        protected HAConnection createConnection(SocketChannel sc) throws IOException {
            return new DefaultHAConnection(DefaultHAService.this, sc);
        }

        @Override
        public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return DefaultAcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    protected abstract class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        private final MessageStoreConfig messageStoreConfig;

        public AcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            this.messageStoreConfig = messageStoreConfig;
            this.socketAddressListen = new InetSocketAddress(messageStoreConfig.getHaListenPort());
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = NetworkUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            if (0 == messageStoreConfig.getHaListenPort()) {
                messageStoreConfig.setHaListenPort(this.serverSocketChannel.socket().getLocalPort());
                log.info("OS picked up {} to listen for HA", messageStoreConfig.getHaListenPort());
            }
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                if (null != this.serverSocketChannel) {
                    this.serverSocketChannel.close();
                }

                if (null != this.selector) {
                    this.selector.close();
                }
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if (k.isAcceptable()) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    DefaultHAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());
                                    try {
                                        HAConnection conn = createConnection(sc);
                                        DefaultHAService.this.addConnection(conn);
                                        conn.start();
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * Create ha connection
         */
        protected abstract HAConnection createConnection(final SocketChannel sc) throws IOException;
    }
}
