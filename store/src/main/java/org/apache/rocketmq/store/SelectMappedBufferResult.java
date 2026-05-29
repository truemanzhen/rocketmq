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
import org.apache.rocketmq.store.logfile.MappedFile;

    // MappedBuffer选择结果：包含从MappedFile读取的数据
    public class SelectMappedBufferResult {

    // 起始偏移量
    private final long startOffset;

    // 数据缓冲区
    private final ByteBuffer byteBuffer;

    // 数据大小
    private int size;

    // 关联的MappedFile
    protected MappedFile mappedFile;

    // 是否在缓存中
    private boolean isInCache = true;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    // 获取数据缓冲区
    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    // 获取数据大小
    public int getSize() {
        return size;
    }

    // 设置数据大小
    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    // 获取关联的MappedFile
    public MappedFile getMappedFile() {
        return mappedFile;
    }

    // 释放资源
    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

    // 检查是否已释放
    public synchronized boolean hasReleased() {
        return this.mappedFile == null;
    }

    // 获取起始偏移量
    public long getStartOffset() {
        return startOffset;
    }

    // 检查数据是否在内存中
    public boolean isInMem() {
        if (mappedFile == null) {
            return true;
        }
        long pos = startOffset - mappedFile.getFileFromOffset();
        return mappedFile.isLoaded(pos, size);
    }

    // 检查是否在缓存中
    public boolean isInCache() {
        return isInCache;
    }

    // 设置缓存状态
    public void setInCache(boolean inCache) {
        isInCache = inCache;
    }
}
