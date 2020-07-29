/*
 * Copyright 2020 StreamThoughts.
 *
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
package io.streamthoughts.kafka.clients.consumer.error.serialization

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 *
 */
interface DeserializationErrorHandler<K, V> {

    sealed class Response<K, V> {
        data class Replace<K, V>(val key: K?, val value: V?): DeserializationErrorHandler.Response<K?, V?>()
        class Fail<K, V>: DeserializationErrorHandler.Response<K?, V?>()
        class Skip<K, V>: DeserializationErrorHandler.Response<K?, V?>()
    }

    /**
     * Handles the [error] that has been thrown while de-serializing the given raw [record].
     */
    fun handle(record: ConsumerRecord<ByteArray, ByteArray>, error: Exception): Response<K?, V?>
}