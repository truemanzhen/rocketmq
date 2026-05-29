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

import java.util.concurrent.atomic.AtomicLong;

    // 引用资源基类：实现引用计数和安全释放
    public abstract class ReferenceResource {
    // 引用计数
    protected final AtomicLong refCount = new AtomicLong(1);
    // 是否可用
    protected volatile boolean available = true;
    // 是否清理完成
    protected volatile boolean cleanupOver = false;
    // 首次关闭时间戳
    private volatile long firstShutdownTimestamp = 0;

    // 增加引用计数
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    // 检查是否可用
    public boolean isAvailable() {
        return this.available;
    }

    // 关闭资源（支持强制关闭）
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            // 超过强制关闭时间，强制释放
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    // 释放资源（减少引用计数）
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            // 引用计数为0时清理资源
            this.cleanupOver = this.cleanup(value);
        }
    }

    // 获取引用计数
    public long getRefCount() {
        return this.refCount.get();
    }

    // 清理资源（子类实现）
    public abstract boolean cleanup(final long currentRef);

    // 检查清理是否完成
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
