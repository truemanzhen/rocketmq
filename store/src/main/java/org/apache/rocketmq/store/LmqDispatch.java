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
package org.apache.rocketmq.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

    // LMQ（轻量级消息队列）分发：处理多队列分发逻辑
    public class LmqDispatch {
    // 每次递增的值
    private static final short VALUE_OF_EACH_INCREMENT = 1;

    // 包装LMQ分发信息：获取各LMQ队列的偏移量
    public static void wrapLmqDispatch(MessageStore messageStore, final MessageExtBrokerInner msg)
        throws ConsumeQueueException {
        String lmqNames = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String[] queueNames = lmqNames.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        Long[] queueOffsets = new Long[queueNames.length];
        // 获取每个LMQ队列的偏移量
        if (messageStore.getMessageStoreConfig().isEnableLmq()) {
            for (int i = 0; i < queueNames.length; i++) {
                if (MixAll.isLmq(queueNames[i])) {
                    queueOffsets[i] = messageStore.getQueueStore().getLmqQueueOffset(queueNames[i], MixAll.LMQ_QUEUE_ID);
                }
            }
        }
        // 将偏移量设置到消息属性中
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            StringUtils.join(queueOffsets, MixAll.LMQ_DISPATCH_SEPARATOR));
        msg.removeWaitStorePropertyString();
    }

    // 更新LMQ队列偏移量
    public static void updateLmqOffsets(MessageStore messageStore, final MessageExtBrokerInner msgInner)
        throws ConsumeQueueException {
        String lmqNames = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String[] queueNames = lmqNames.split(MixAll.LMQ_DISPATCH_SEPARATOR);
        // 递增每个LMQ队列的偏移量
        for (String queueName : queueNames) {
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                messageStore.getQueueStore().increaseLmqOffset(queueName, MixAll.LMQ_QUEUE_ID, VALUE_OF_EACH_INCREMENT);
            }
        }
    }
}
