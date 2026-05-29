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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Remoting服务端接口：定义服务端的网络通信操作。
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li>注册请求处理器</li>
 *   <li>同步/异步/单向调用</li>
 *   <li>写入响应</li>
 * </ul>
 *
 * @see NettyRemotingServer
 * @see NettyRequestProcessor
 */
public interface RemotingServer extends RemotingService {

    // 注册请求处理器
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    // 注册默认请求处理器
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    // 获取本地监听端口
    int localListenPort();

    // 获取指定请求码的处理器
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    // 获取默认处理器
    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    // 创建新的RemotingServer
    RemotingServer newRemotingServer(int port);

    // 移除RemotingServer
    void removeRemotingServer(int port);

    // 同步调用
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
        RemotingTimeoutException;

    // 异步调用
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    // 单向调用（不等待响应）
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException;

    // 写入响应
    void writeResponse(final Channel channel, final RemotingCommand request,
        final RemotingCommand response, final java.util.function.Consumer<io.netty.util.concurrent.Future<?>> callback);

}
