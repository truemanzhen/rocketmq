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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;

    // 客户端连接管理服务：监听Channel事件，管理Producer和Consumer的连接
    public class ClientHousekeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    // 定时任务线程池
    private ScheduledExecutorService scheduledExecutorService;

    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
        scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new ThreadFactoryImpl("ClientHousekeepingScheduledThread", brokerController.getBrokerIdentity()));
    }

    // 启动定时扫描不活跃Channel
    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                ClientHousekeepingService.this.scanExceptionChannel();
            } catch (Throwable e) {
                log.error("Error occurred when scan not active client channels.", e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    // 扫描不活跃的Channel
    private void scanExceptionChannel() {
        this.brokerController.getProducerManager().scanNotActiveChannel();
        this.brokerController.getConsumerManager().scanNotActiveChannel();
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    // Channel连接事件
    public void onChannelConnect(String remoteAddr, Channel channel) {
        this.brokerController.getBrokerStatsManager().incChannelConnectNum();
    }

    @Override
    // Channel关闭事件：清理Producer和Consumer的连接
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getBrokerStatsManager().incChannelCloseNum();
    }

    @Override
    // Channel异常事件：清理Producer和Consumer的连接
    public void onChannelException(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getBrokerStatsManager().incChannelExceptionNum();
    }

    @Override
    // Channel空闲事件：清理Producer和Consumer的连接
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getBrokerStatsManager().incChannelIdleNum();
    }

    @Override
    // Channel激活事件（空实现）
    public void onChannelActive(String remoteAddr, Channel channel) {

    }
}
