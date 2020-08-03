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
package io.streamthoughts.kafka.clients.consumer.listener

import io.streamthoughts.kafka.clients.consumer.ConsumerTask
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

fun <K, V> noop() : ConsumerBatchRecordsListener<K, V> {
    return DelegatingConsumerBatchRecordsListener { _: ConsumerTask, _: ConsumerRecords<K?, V?> -> }
}

@JvmName("onConsumedRecord")
fun <K, V> forEach(callback: (consumerTask: ConsumerTask, record: ConsumerRecord<K?, V?>) -> Unit)
        : ConsumerBatchRecordsListener<K, V> {
    return DelegatingConsumerBatchRecordsListener { task, records ->
        records.onEach { callback(task, it) }
    }
}

@JvmName("onConsumedValueRecordWithKey")
fun <K, V> forEach(callback: (consumerTask: ConsumerTask, record: Pair<K?, V?>) -> Unit)
        : ConsumerBatchRecordsListener<K, V> {
    return DelegatingConsumerBatchRecordsListener { task, records ->
        records.onEach { callback(task, Pair(it.key(), it.value())) }
    }
}

@JvmName("onConsumedValueRecord")
fun <K, V> forEach(callback: (consumerTask: ConsumerTask, value: V?) -> Unit)
        : ConsumerBatchRecordsListener<K, V> {
    return DelegatingConsumerBatchRecordsListener { task, records ->
        records.onEach { callback(task, it.value()) }
    }
}

interface ConsumerBatchRecordsListener<K, V> {

    /**
     * This method is invoked after the [consumerTask] has polled a non-empty batch of [records].
     *
     * @see [org.apache.kafka.clients.consumer.Consumer.poll]
     */
    fun handle(consumerTask: ConsumerTask, records: ConsumerRecords<K?, V?>): Unit
}

private class DelegatingConsumerBatchRecordsListener<K, V>(
    private val callback: (consumerTask: ConsumerTask, record: ConsumerRecords<K?, V?>) -> Unit
): ConsumerBatchRecordsListener<K, V> {

    override fun handle(consumerTask: ConsumerTask, records: ConsumerRecords<K?, V?>) {
        callback(consumerTask, records)
    }
}