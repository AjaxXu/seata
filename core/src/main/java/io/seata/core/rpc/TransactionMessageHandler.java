/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.rpc;

import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.AbstractResultMessage;

/**
 * To handle the received RPC message on upper level.
 * 事务消息处理器，在上一层处理接收到的RPC消息
 *
 * @author jimin.jm @alibaba-inc.com
 */
public interface TransactionMessageHandler {

    /**
     * On a request received.
     * 收到请求消息时调用
     *
     * @param request received request message
     * @param context context of the RPC
     * @return response to the request
     */
    AbstractResultMessage onRequest(AbstractMessage request, RpcContext context);

    /**
     * On a response received.
     * 收到响应结果时调用
     *
     * @param response received response message
     * @param context  context of the RPC
     */
    void onResponse(AbstractResultMessage response, RpcContext context);

}
