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

    // 消息写入结果：包含写入状态和追加结果
    public class PutMessageResult {
    // 写入状态（成功、刷盘超时、从节点不可用等）
    private PutMessageStatus putMessageStatus;
    // 追加消息结果（物理偏移量、队列偏移量、写入大小等）
    private AppendMessageResult appendMessageResult;
    // 是否为远程写入（通过EscapeBridge转发到其他Broker）
    private boolean remotePut = false;

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult,
        boolean remotePut) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
        this.remotePut = remotePut;
    }

    // 判断消息是否写入成功
    public boolean isOk() {
        if (remotePut) {
            // 远程写入：刷盘超时和从节点不可用也算成功（消息已写入Master）
            return putMessageStatus == PutMessageStatus.PUT_OK || putMessageStatus == PutMessageStatus.FLUSH_DISK_TIMEOUT
                || putMessageStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT || putMessageStatus == PutMessageStatus.SLAVE_NOT_AVAILABLE;
        } else {
            // 本地写入：检查追加结果
            return this.appendMessageResult != null && this.appendMessageResult.isOk();
        }

    }

    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }

    public void setAppendMessageResult(AppendMessageResult appendMessageResult) {
        this.appendMessageResult = appendMessageResult;
    }

    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }

    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }

    public boolean isRemotePut() {
        return remotePut;
    }

    public void setRemotePut(boolean remotePut) {
        this.remotePut = remotePut;
    }

    @Override
    public String toString() {
        return "PutMessageResult [putMessageStatus=" + putMessageStatus + ", appendMessageResult="
            + appendMessageResult + ", remotePut=" + remotePut + "]";
    }

}
