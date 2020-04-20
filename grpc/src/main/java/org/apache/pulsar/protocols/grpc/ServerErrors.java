/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.protocols.grpc;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.protocols.grpc.api.CommandSubscribe;
import org.apache.pulsar.protocols.grpc.api.KeySharedMeta;
import org.apache.pulsar.protocols.grpc.api.KeySharedMode;
import org.apache.pulsar.protocols.grpc.api.ServerError;

public class ServerErrors {

    public static ServerError convertServerError(PulsarApi.ServerError serverError) {
        if(serverError == null) {
            return null;
        }
        switch(serverError) {
            case MetadataError:
                return ServerError.MetadataError;
            case PersistenceError:
                return ServerError.PersistenceError;
            case AuthenticationError:
                return ServerError.AuthenticationError;
            case AuthorizationError:
                return ServerError.AuthorizationError;
            case ConsumerBusy:
                return ServerError.ConsumerBusy;
            case ServiceNotReady:
                return ServerError.ServiceNotReady;
            case ProducerBlockedQuotaExceededError:
                return ServerError.ProducerBlockedQuotaExceededError;
            case ProducerBlockedQuotaExceededException:
                return ServerError.ProducerBlockedQuotaExceededException;
            case ChecksumError:
                return ServerError.ChecksumError;
            case UnsupportedVersionError:
                return ServerError.UnsupportedVersionError;
            case TopicNotFound:
                return ServerError.TopicNotFound;
            case SubscriptionNotFound:
                return ServerError.SubscriptionNotFound;
            case ConsumerNotFound:
                return ServerError.ConsumerNotFound;
            case TooManyRequests:
                return ServerError.TooManyRequests;
            case TopicTerminatedError:
                return ServerError.TopicTerminatedError;
            case ProducerBusy:
                return ServerError.ProducerBusy;
            case InvalidTopicName:
                return ServerError.InvalidTopicName;
            case IncompatibleSchema:
                return ServerError.IncompatibleSchema;
            case ConsumerAssignError:
                return ServerError.ConsumerAssignError;
            case TransactionCoordinatorNotFound:
                return ServerError.TransactionCoordinatorNotFound;
            case InvalidTxnStatus:
                return ServerError.InvalidTxnStatus;
            case UnknownError:
            default:
                return ServerError.UnknownError;
        }
    }

    public static SubType convertSubscribeSubType(CommandSubscribe.SubType subType) {
        if(subType == null) {
            return null;
        }
        switch (subType) {
            case Shared:
                return SubType.Shared;
            case Failover:
                return SubType.Failover;
            case Exclusive:
                return SubType.Exclusive;
            case Key_Shared:
                return SubType.Key_Shared;
            default:
                throw new IllegalStateException("Unexpected subscribe subtype: " + subType);
        }
    }

    public static InitialPosition convertSubscribeInitialPosition(CommandSubscribe.InitialPosition initialPosition) {
        if(initialPosition == null) {
            return null;
        }
        switch (initialPosition) {
            case Latest:
                return InitialPosition.Latest;
            case Earliest:
                return InitialPosition.Earliest;
            default:
                throw new IllegalStateException("Unexpected subscribe initial position : " + initialPosition);
        }
    }

    public static PulsarApi.KeySharedMode convertKeySharedMode(KeySharedMode mode) {
        if(mode == null) {
            return null;
        }
        switch (mode) {
            case STICKY:
                return PulsarApi.KeySharedMode.STICKY;
            case AUTO_SPLIT:
                return PulsarApi.KeySharedMode.AUTO_SPLIT;
            default:
                throw new IllegalStateException("Unexpected key shared mode: " + mode);
        }
    }

    public static PulsarApi.KeySharedMeta convertKeySharedMeta(KeySharedMeta meta) {
        if (meta == null) {
            return null;
        }
        PulsarApi.KeySharedMeta.Builder builder = PulsarApi.KeySharedMeta.newBuilder()
                .setKeySharedMode(convertKeySharedMode(meta.getKeySharedMode()));
        meta.getHashRangesList().stream()
                .map(intRange -> PulsarApi.IntRange.newBuilder()
                        .setStart(intRange.getStart())
                        .setEnd(intRange.getEnd()))
                .forEach(builder::addHashRanges);
        return builder.build();
    }
}
