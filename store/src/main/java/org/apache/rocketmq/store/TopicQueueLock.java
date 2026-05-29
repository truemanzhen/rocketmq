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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

    // TopicQueue锁：按TopicQueue粒度加锁，保证同一队列的消息顺序写入
    public class TopicQueueLock {
    // 锁数组大小
    private final int size;
    // 锁列表
    private final List<Lock> lockList;

    // 默认构造函数（32个锁）
    public TopicQueueLock() {
        this.size = 32;
        this.lockList = new ArrayList<>(32);
        for (int i = 0; i < this.size; i++) {
            this.lockList.add(new ReentrantLock());
        }
    }

    // 自定义大小构造函数
    public TopicQueueLock(int size) {
        this.size = size;
        this.lockList = new ArrayList<>(size);
        for (int i = 0; i < this.size; i++) {
            this.lockList.add(new ReentrantLock());
        }
    }

    // 加锁（根据TopicQueueKey的hashCode选择锁）
    public void lock(String topicQueueKey) {
        Lock lock = this.lockList.get((topicQueueKey.hashCode() & 0x7fffffff) % this.size);
        lock.lock();
    }

    // 解锁
    public void unlock(String topicQueueKey) {
        Lock lock = this.lockList.get((topicQueueKey.hashCode() & 0x7fffffff) % this.size);
        lock.unlock();
    }
}
