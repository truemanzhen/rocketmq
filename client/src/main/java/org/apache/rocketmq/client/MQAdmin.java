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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;

/**
 * MQ管理基础接口：定义消息队列管理的基本操作。
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li>Topic管理：创建Topic</li>
 *   <li>偏移量查询：查询最大、最小、指定时间的偏移量</li>
 *   <li>消息查询：按Key、时间范围查询消息</li>
 *   <li>消息查看：根据消息ID查看消息</li>
 * </ul>
 *
 * @see org.apache.rocketmq.client.producer.DefaultMQProducer
 * @see org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
 */
public interface MQAdmin {
    // 创建Topic
    void createTopic(final String key, final String newTopic, final int queueNum, Map<String, String> attributes)
        throws MQClientException;

    // 创建Topic（带系统标志）
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes)
        throws MQClientException;

    // 根据时间戳查找偏移量
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    // 获取最大偏移量
    long maxOffset(final MessageQueue mq) throws MQClientException;

    // 获取最小偏移量
    long minOffset(final MessageQueue mq) throws MQClientException;

    // 获取最早消息存储时间
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    // 按Key查询消息
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
        final long end) throws MQClientException, InterruptedException;

    /**
     * @return The {@code MessageExt} of given msgId
     */
    MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}