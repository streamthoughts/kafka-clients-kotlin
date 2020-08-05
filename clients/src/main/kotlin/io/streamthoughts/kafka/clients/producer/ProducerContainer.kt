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
import io.streamthoughts.kafka.clients.producer.callback.OnSendErrorCallback
import io.streamthoughts.kafka.clients.producer.callback.OnSendSuccessCallback
import io.streamthoughts.kafka.clients.producer.callback.ProducerSendCallback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.Serializer
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Future


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

interface ProducerContainer<K, V>: Closeable {

    enum class State {
        /**
         * The [ProducerContainer] is created.
         */
        CREATED,

        /**
         * The [ProducerContainer] is initialized and can be used for sending records.
         */
        STARTED,

        /**
         * The [ProducerContainer] is closing
         */
        PENDING_SHUTDOWN,

        /**
         * The [ProducerContainer] is closed.
         */
        CLOSED,
    }

    interface Builder<K, V> {
        /**
         * Configure this [ProducerContainer].
         */
        fun configure(init: KafkaProducerConfigs.() -> Unit): Builder<K, V>

        /**
         * Sets the [producerFactory] to be used for creating a new [Producer] client.
         */
        fun producerFactory(producerFactory: ProducerFactory): Builder<K, V>

        /**
         * Set the default topic to send records
         */
        fun defaultTopic(topic: String): Builder<K, V>

        /**
         * Set the default [callback] to invoke when an error happen while sending a record.
         */
        fun onSendError(callback: OnSendErrorCallback<K, V>): Builder<K, V>

        /**
         * Set the default [callback] to invoke when a record has been sent successfully.
         */
        fun onSendSuccess(callback: OnSendSuccessCallback<K, V>): Builder<K, V>

        /**
         * Set the default [callback] to be invoked after a sent record completes either successfully or unsuccessfully.
         *
         * @see onSendError
         * @see onSendSuccess
         */
        fun onSendCallback(callback: ProducerSendCallback<K, V>): Builder<K, V>

        /**
         * Set the [serializer] to be used for serializing the record-key.
         */
        fun keySerializer(serializer: Serializer<K>): Builder<K, V>

        /**
         * Set the [serializer] to be used for serializing the record-value.
         */
        fun valueSerializer(serializer: Serializer<V>): Builder<K, V>
    }

    /**
     * Asynchronously send a record for the given [value] to the given to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     * @return  a [Future] of [SendResult]
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
     * Asynchronously send a record for the given [key] and [value] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     * @return  a [Future] of [SendResult]
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
     * Asynchronously send the given key-value [pair] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     * @return a [Future] of [SendResult]
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
     * Asynchronously send all the given key-value [pairs] to the given [topic] (or the default one if null is given)
     * and [partition] with the given [timestamp].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     * @return a [Future] of [SendResult]
     */
    fun send(pairs: Collection<Pair<K, V>>,
             topic: String? = null,
             partition: Int? = null,
             timestamp: Instant? = null,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<List<SendResult<K?, V?>>>

    /**
     * Asynchronously send the given [record].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     * @return a [Future] of [SendResult]
     */
    fun send(record: KafkaRecord<K?, V?>,
             onSuccess: OnSendSuccessCallback<K, V>? = null,
             onError: OnSendErrorCallback<K, V>? = null) : Future<SendResult<K?, V?>>
    /**
     * Asynchronously send the given [record].
     *
     * Then, optionally invoke the specific given [onSuccess] callback when the record has been acknowledge.
     * Otherwise invoke [onError] if an error happen while sending.
     *
     * @see Producer.send
     * @return a [Future] of [SendResult]
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
     * @return the [State] of this container.
     */
    fun state(): State

    /**
     * Close this [ProducerContainer].
     *
     * @see [Producer.close].
     */
    override fun close() {
        close(Duration.ofMillis(Long.MAX_VALUE))
    }

    /**
     * Close this [ProducerContainer].
     *
     * @see [Producer.close].
     */
    fun close(timeout: Duration)
}
