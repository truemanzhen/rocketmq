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
package org.apache.rocketmq.store.lock;

import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

    // 退避自旋锁：支持自适应调整自旋次数的自旋锁
    public class BackOffSpinLock implements AdaptiveBackOffSpinLock {

    // 自旋锁状态
    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    // 最优自旋次数
    private int optimalDegree;

    // 初始自旋次数
    private final static int INITIAL_DEGREE = 1000;

    // 最大自旋次数
    private final static int MAX_OPTIMAL_DEGREE = 10000;

    // 退避次数统计
    private final List<AtomicInteger> numberOfRetreat;

    public BackOffSpinLock() {
        this.optimalDegree = INITIAL_DEGREE;

        numberOfRetreat = new ArrayList<>(2);
        numberOfRetreat.add(new AtomicInteger(0));
        numberOfRetreat.add(new AtomicInteger(0));
    }

    @Override
    // 加锁：自旋尝试获取锁
    public void lock() {
        int spinDegree = this.optimalDegree;
        while (true) {
            // 自旋尝试
            for (int i = 0; i < spinDegree; i++) {
                if (this.putMessageSpinLock.compareAndSet(true, false)) {
                    return;
                }
            }
            // 自旋失败，记录退避次数
            numberOfRetreat.get(LocalTime.now().getSecond() % 2).getAndIncrement();
            try {
                // 让出CPU时间片
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    // 解锁
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }

    @Override
    // 更新配置
    public void update(MessageStoreConfig messageStoreConfig) {
        this.optimalDegree = messageStoreConfig.getSpinLockCollisionRetreatOptimalDegree();
    }

    // 获取最优自旋次数
    public int getOptimalDegree() {
        return this.optimalDegree;
    }

    // 设置最优自旋次数
    public void setOptimalDegree(int optimalDegree) {
        this.optimalDegree = optimalDegree;
    }

    // 判断是否可以自适应调整
    public boolean isAdapt() {
        return optimalDegree < MAX_OPTIMAL_DEGREE;
    }

    // 自适应调整自旋次数
    public synchronized void adapt(boolean isRise) {
        if (isRise) {
            // 增加自旋次数
            if (optimalDegree * 2 <= MAX_OPTIMAL_DEGREE) {
                optimalDegree *= 2;
            } else {
                if (optimalDegree + INITIAL_DEGREE <= MAX_OPTIMAL_DEGREE) {
                    optimalDegree += INITIAL_DEGREE;
                }
            }
        } else {
            // 减少自旋次数
            if (optimalDegree >= 2 * INITIAL_DEGREE) {
                optimalDegree -= INITIAL_DEGREE;
            }
        }
    }

    public int getNumberOfRetreat(int pos) {
        return numberOfRetreat.get(pos).get();
    }

    public void setNumberOfRetreat(int pos, int size) {
        this.numberOfRetreat.get(pos).set(size);
    }
}
