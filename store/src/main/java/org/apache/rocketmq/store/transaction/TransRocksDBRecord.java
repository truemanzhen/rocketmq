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
package org.apache.rocketmq.store.transaction;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

    // RocksDB事务记录：存储事务消息的索引信息
    public class TransRocksDBRecord {
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    // 值长度（8字节）
    public static final int VALUE_LENGTH = Integer.BYTES + Integer.BYTES;
    // Key分隔符
    private static final String KEY_SPLIT = "@";
    // CommitLog物理偏移量
    protected long offsetPy;
    // Topic名称
    private String topic;
    // 消息唯一Key
    private String uniqKey;
    // 检查次数
    private int checkTimes = 0;
    // 消息大小
    private int sizePy;
    // 是否为操作记录
    private boolean isOp;
    // 是否删除
    private boolean delete;
    // 消息对象
    private MessageExt messageExt;

    public TransRocksDBRecord(long offsetPy, String topic, String uniqKey, int sizePy, int checkTimes) {
        this.offsetPy = offsetPy;
        this.topic = topic;
        this.uniqKey = uniqKey;
        this.sizePy = sizePy;
        this.checkTimes = checkTimes;
    }

    public TransRocksDBRecord(long offsetPy, String topic, String uniqKey, boolean isOp) {
        this.offsetPy = offsetPy;
        this.topic = topic;
        this.uniqKey = uniqKey;
        this.isOp = isOp;
    }

    public TransRocksDBRecord() {}

    // 编码Key：offsetPy + @topic@uniqKey
    public byte[] getKeyBytes() {
        if (offsetPy < 0L || StringUtils.isEmpty(topic) || StringUtils.isEmpty(uniqKey)) {
            return null;
        }
        byte[] keySuffixBytes = (KEY_SPLIT + topic + KEY_SPLIT + uniqKey).getBytes(StandardCharsets.UTF_8);
        int keyLength = Long.BYTES + keySuffixBytes.length;
        return ByteBuffer.allocate(keyLength).putLong(offsetPy).put(keySuffixBytes).array();
    }

    // 编码Value：checkTimes + sizePy
    public byte[] getValueBytes() {
        if (checkTimes < 0 || sizePy <= 0) {
            logError.error("TransRocksDBRecord getValueBytes error, checkTimes: {}, sizePy: {}", checkTimes, sizePy);
            return null;
        }
        return ByteBuffer.allocate(VALUE_LENGTH).putInt(checkTimes).putInt(sizePy).array();
    }

    // 解码RocksDB记录
    public static TransRocksDBRecord decode(byte[] key, byte[] value) {
        if (null == key || key.length <= Long.BYTES || null == value || value.length != VALUE_LENGTH) {
            logError.error("TransRocksDBRecord decode param error, key: {}, value: {}", key, value);
            return null;
        }
        TransRocksDBRecord transRocksDBRecord = null;
        try {
            transRocksDBRecord = new TransRocksDBRecord();
            ByteBuffer keyByteBuffer = ByteBuffer.wrap(key);
            // 读取物理偏移量
            transRocksDBRecord.setOffsetPy(keyByteBuffer.getLong());
            byte[] keySuffix = new byte[key.length - Long.BYTES];
            keyByteBuffer.get(keySuffix);
            // 解析Topic和UniqKey
            String[] keySuffixSplit = new String(keySuffix, StandardCharsets.UTF_8).split(KEY_SPLIT);
            if (keySuffixSplit.length != 3) {
                logError.error("TransRocksDBRecord decode keySuffixSplit parse error");
                return null;
            }
            transRocksDBRecord.setTopic(keySuffixSplit[1]);
            transRocksDBRecord.setUniqKey(keySuffixSplit[2]);
            ByteBuffer valueByteBuffer = ByteBuffer.wrap(value);
            // 读取检查次数和消息大小
            transRocksDBRecord.setCheckTimes(valueByteBuffer.getInt());
            transRocksDBRecord.setSizePy(valueByteBuffer.getInt());
        } catch (Exception e) {
            logError.error("TransRocksDBRecord decode error, valueLength: {}, error: {}", value.length, e.getMessage());
            return null;
        }
        return transRocksDBRecord;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public void setUniqKey(String uniqKey) {
        this.uniqKey = uniqKey;
    }

    public int getCheckTimes() {
        return checkTimes;
    }

    public void setCheckTimes(int checkTimes) {
        this.checkTimes = checkTimes;
    }

    public int getSizePy() {
        return sizePy;
    }

    public void setSizePy(int sizePy) {
        this.sizePy = sizePy;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public void setOffsetPy(long offsetPy) {
        this.offsetPy = offsetPy;
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }

    public void setMessageExt(MessageExt messageExt) {
        this.messageExt = messageExt;
    }

    public boolean isOp() {
        return isOp;
    }

    public void setOp(boolean op) {
        isOp = op;
    }

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
}
