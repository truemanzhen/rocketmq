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
import java.util.ArrayList;
import java.util.List;

    // 消息查询结果：包含按Key查询到的消息数据
    public class QueryMessageResult {

    // 消息MappedBuffer列表
    private final List<SelectMappedBufferResult> messageMapedList =
        new ArrayList<>(100);

    // 消息ByteBuffer列表
    private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);
    // 索引最后更新时间戳
    private long indexLastUpdateTimestamp;
    // 索引最后更新物理偏移量
    private long indexLastUpdatePhyoffset;

    // 缓冲区总大小
    private int bufferTotalSize = 0;

    // 添加消息
    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
    }

    // 释放所有资源
    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    // 获取索引最后更新时间戳
    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }

    // 设置索引最后更新时间戳
    public void setIndexLastUpdateTimestamp(long indexLastUpdateTimestamp) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
    }

    // 获取索引最后更新物理偏移量
    public long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }

    // 设置索引最后更新物理偏移量
    public void setIndexLastUpdatePhyoffset(long indexLastUpdatePhyoffset) {
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }

    // 获取消息ByteBuffer列表
    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    // 获取缓冲区总大小
    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    // 获取消息MappedBuffer列表
    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }
}
