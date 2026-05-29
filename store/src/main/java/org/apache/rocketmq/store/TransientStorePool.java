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

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.netty.util.internal.PlatformDependent;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.util.LibC;

public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private volatile boolean isRealCommit = true;

    public TransientStorePool(final int poolSize, final int fileSize) {
        this.poolSize = poolSize;
        this.fileSize = fileSize;
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    // 初始化堆外内存池：预分配堆外内存并锁定，避免被交换到磁盘
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            // 分配堆外内存
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            // 使用mlock锁定内存，防止被操作系统换出
            final long address = PlatformDependent.directBufferAddress(byteBuffer);
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    // 销毁内存池：解锁并释放堆外内存
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = PlatformDependent.directBufferAddress(byteBuffer);
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    // 归还ByteBuffer到内存池
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    // 从内存池借用ByteBuffer
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        // 当剩余缓冲区不足40%时打印警告
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        return availableBuffers.size();
    }

    public boolean isRealCommit() {
        return isRealCommit;
    }

    public void setRealCommit(boolean realCommit) {
        isRealCommit = realCommit;
    }
}
