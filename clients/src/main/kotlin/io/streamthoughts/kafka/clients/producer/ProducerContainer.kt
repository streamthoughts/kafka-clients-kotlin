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
package io.streamthoughts.kafka.clients.producer

import io.streamthoughts.kafka.clients.KafkaRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.Serializer
import java.time.Instant
import java.util.concurrent.Future


typealias OnSendErrorCallback<K, V> = (producer: Producer<K, V>, record: ProducerRecord<K?, V?>, error: Exception) -> Unit
typealias OnSendSuccessCallback<K, V> = (producer: Producer<K, V>, record: ProducerRecord<K?, V?>, metadata: RecordMetadata) -> Unit


sealed class TransactionResult
/**
 * The transaction has been aborted due to an exception.
 * @see Producer.abortTransaction
 */
data class AbortedTransactionResult( val exception: Exception): TransactionResult()

/**
 * The transaction throws an unrecoverable error. Hence the [Producer] has been closed.
 *
 */
data class UnrecoverableErrorTransactionResult( val exception: Exception): TransactionResult()

/**
 * The transaction has been successfully committed
 * @see Producer.commitTransaction
 */
object CommittedTransactionResult: TransactionResult()

interface ProducerContainer<K, V> {

    /**
     * Configure this [ProducerContainer].
     */
    fun configure(init: KafkaProducerConfigs.() -> Unit):  KafkaProducerContainer<K, V>

    /**
     * Set the default topic to send records
     */
    fun defaultTopic(topic: String):  KafkaProducerContainer<K, V>

    /**
     * Set the default [callback] to invoke when an error happen while sending a record.
     */
    fun onSendError(callback: OnSendErrorCallback<K, V>):  KafkaProducerContainer<K, V>

    /**
     * Set the default [callback] to invoke when a record has been sent successfully.
     */
    fun onSendSuccess(callback: OnSendSuccessCallback<K, V>):  KafkaProducerContainer<K, V>

    /**
     * Set the [serializer] to be used for serializing the record-key.
     */
    fun keySerializer(serializer: Serializer<K>):  KafkaProducerContainer<K, V>

    /**
     * Set the [serializer] to be used for serializing the record-value.
     */
    fun valueSerializer(serializer: Serializer<V>):  KafkaProducerContainer<K, V>

    /**
     * Send a record for the given [value] to the given to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     */
    fun send(value : V,
             topic: String? = null,
             partition: Int? = null,
             timestamp: Instant? = null,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<SendResult<K?, V?>> {
        return send(null, value, topic, partition, timestamp, onSuccess, onError)
    }

    /**
     * Send a record for the given [key] and [value] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     */
    fun send(key: K?= null,
             value: V? = null,
             topic: String? = null,
             partition: Int? = null,
             timestamp: Instant? = null,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<SendResult<K?, V?>> {
        return send(KafkaRecord<K?, V?>(key, value, topic, partition, timestamp), onSuccess, onError)
    }

    /**
     * Send the given key-value [pair] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     */
    fun send(pair: Pair<K, V?>,
             topic: String? = null,
             partition: Int? = null,
             timestamp: Instant? = null,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null)  : Future<SendResult<K?, V?>> {
        return send(pair.first, pair.second, topic, partition, timestamp, onSuccess, onError)
    }

    /**
     * Send all the given key-value [pairs] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     */
    fun send(pairs: Collection<Pair<K, V>>,
             topic: String? = null,
             partition: Int? = null,
             timestamp: Instant? = null,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<List<SendResult<K?, V?>>>

    /**
     * Send the given [record] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     */
    fun send(record: KafkaRecord<K?, V?>,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<SendResult<K?, V?>>
    /**
     * Send the given [record] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     */
    fun send(record: ProducerRecord<K?, V?>,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<SendResult<K?, V?>>

    /**
     * Executes the given [action] with the underlying [Producer].
     */
    fun <T> execute(action: (producer: Producer<K, V>) -> T): T

    /**
     * Executes the given [action] in a producer transaction.
     */
    fun runTx(action: (ProducerContainer<K, V>) -> Unit): TransactionResult

    /**
     * @see Producer.metrics
     */
    fun metrics(topic: String): Map<MetricName, Metric>

    /**
     * @see Producer.partitionsFor
     */
    fun partitionsFor(topic: String): List<PartitionInfo>

    /**
     * Initialize this [ProducerContainer].
     */
    fun init()

    /**
     * Flush the [Producer].
     */
    fun flush()

    /**
     * Close this [ProducerContainer].
     *
     * @see [Producer.close].
     */
    fun close()
}
