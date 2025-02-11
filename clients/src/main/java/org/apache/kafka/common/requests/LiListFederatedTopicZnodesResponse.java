/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.message.LiListFederatedTopicZnodesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiListFederatedTopicZnodesResponse extends AbstractResponse {
    private final LiListFederatedTopicZnodesResponseData data;
    private final short version;

    public LiListFederatedTopicZnodesResponse(LiListFederatedTopicZnodesResponseData data, short version) {
        super(ApiKeys.LI_LIST_FEDERATED_TOPIC_ZNODES);
        this.data = data;
        this.version = version;
    }

    @Override
    public LiListFederatedTopicZnodesResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public static LiListFederatedTopicZnodesResponse parse(ByteBuffer buffer, short version) {
        return new LiListFederatedTopicZnodesResponse(
            new LiListFederatedTopicZnodesResponseData(new ByteBufferAccessor(buffer), version), version
        );
    }

    public static LiListFederatedTopicZnodesResponse prepareResponse(Errors error, int throttleTimeMs, short version) {
        LiListFederatedTopicZnodesResponseData data = new LiListFederatedTopicZnodesResponseData();
        data.setErrorCode(error.code());
        data.setThrottleTimeMs(throttleTimeMs);
        return new LiListFederatedTopicZnodesResponse(data, version);
    }

    public short version() {
        return version;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
