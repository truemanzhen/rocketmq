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

package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;
import org.rocksdb.RocksDBException;

    // ConsumeQueue接口：定义消费队列的操作契约
    public interface ConsumeQueueInterface extends FileQueueLifeCycle {
    // 获取Topic名称
    String getTopic();

    // 获取队列ID
    int getQueueId();

    // 从指定偏移量开始迭代
    ReferredIterator<CqUnit> iterateFrom(long startIndex);

    // 从指定偏移量开始迭代指定数量
    ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws RocksDBException;

    // 获取指定索引的CqUnit
    CqUnit get(long index);

    // 获取指定索引的CqUnit和存储时间
    Pair<CqUnit, Long> getCqUnitAndStoreTime(long index);

    // 获取最早的CqUnit和存储时间
    Pair<CqUnit, Long> getEarliestUnitAndStoreTime();

    // 获取最早的CqUnit
    CqUnit getEarliestUnit();

    // 获取最新的CqUnit
    CqUnit getLatestUnit();

    // 获取最后的CommitLog偏移量
    long getLastOffset();

    // 获取队列中的最小偏移量
    long getMinOffsetInQueue();

    /**
     * Get max offset(index) in queue
     * @return the max offset(index) in queue
     */
    long getMaxOffsetInQueue();

    /**
     * Get total message count
     * @return total message count
     */
    long getMessageTotalInQueue();

    /**
     * Get the message whose timestamp is the smallest, greater than or equal to the given time.
     * @param timestamp timestamp
     * @return the offset(index)
     */
    long getOffsetInQueueByTime(final long timestamp);

    /**
     * Get the message whose timestamp is the smallest, greater than or equal to the given time and when there are more
     * than one message satisfy the condition, decide which one to return based on boundaryType.
     * @param timestamp    timestamp
     * @param boundaryType Lower or Upper
     * @return the offset(index)
     */
    long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType);

    /**
     * The max physical offset of commitlog has been dispatched to this queue.
     * It should be exclusive.
     *
     * @return the max physical offset point to commitlog
     */
    long getMaxPhysicOffset();

    /**
     * Usually, the cq files are not exactly consistent with the commitlog, there maybe some redundant data in the first
     * cq file.
     *
     * @return the minimal effective pos of the cq file.
     */
    long getMinLogicOffset();

    /**
     * Get cq type
     * @return cq type
     */
    CQType getCQType();

    /**
     * Gets the occupied size of CQ file on disk
     * @return total size
     */
    long getTotalSize();

    /**
     * Get the unit size of this CQ which is different in different CQ impl
     * @return cq unit size
     */
    int getUnitSize();

    /**
     * Correct min offset by min commit log offset.
     * @param minCommitLogOffset min commit log offset
     */
    void correctMinOffset(long minCommitLogOffset);

    /**
     * Do dispatch.
     * @param request the request containing dispatch information.
     */
    void putMessagePositionInfoWrapper(DispatchRequest request);

    /**
     * Assign queue offset.
     * @param queueOffsetAssigner the delegated queue offset assigner
     * @param msg message itself
     * @throws RocksDBException only in rocksdb mode
     */
    void assignQueueOffset(QueueOffsetOperator queueOffsetAssigner, MessageExtBrokerInner msg) throws RocksDBException;

    /**
     * Increase queue offset.
     * @param queueOffsetAssigner the delegated queue offset assigner
     * @param msg message itself
     * @param messageNum message number
     */
    void increaseQueueOffset(QueueOffsetOperator queueOffsetAssigner, MessageExtBrokerInner msg, short messageNum);

    /**
     * Estimate number of records matching given filter.
     *
     * @param from Lower boundary, inclusive.
     * @param to Upper boundary, inclusive.
     * @param filter Specified filter criteria
     * @return Number of matching records.
     */
    long estimateMessageCount(long from, long to, MessageFilter filter);

    /**
     * Initialize cq and set max offset and min offset to given offset
     *
     * @param offset       set max and min offset to given offset
     * @param minPhyOffset min physical offset, used to correct min offset
     */
    void initializeWithOffset(long offset, long minPhyOffset);
}
