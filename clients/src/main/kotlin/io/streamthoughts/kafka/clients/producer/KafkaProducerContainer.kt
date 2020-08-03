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

import ch.qos.logback.classic.Level
import io.streamthoughts.kafka.clients.KafkaRecord
import io.streamthoughts.kafka.clients.loggerFor
import io.streamthoughts.kafka.clients.producer.callback.OnSendErrorCallback
import io.streamthoughts.kafka.clients.producer.callback.OnSendSuccessCallback
import io.streamthoughts.kafka.clients.producer.callback.ProducerSendCallback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.Logger
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

/**
 * The default kafka-based [ProducerContainer] implementation
 */
class KafkaProducerContainer<K, V> private constructor(
   private val configs: KafkaProducerConfigs,
   private val onSendCallback: ProducerSendCallback<K, V>,
   private val keySerializer: Serializer<K> ?= null,
   private val valueSerializer: Serializer<V> ?= null,
   private val producerFactory: ProducerFactory? = null,
   private val defaultTopic: String? = null
): ProducerContainer<K, V> {

    companion object {
        private val Log: Logger = loggerFor(ProducerContainer::class.java)
        private val numInstances = AtomicInteger(0)
        private const val defaultClientIdPrefix = "producer-"

        private fun computeNextClientId(producerConfigs: Map<String, Any?>): String {
            val clientIdPrefix = producerConfigs[ProducerConfig.CLIENT_ID_CONFIG] ?: defaultClientIdPrefix
            return "$clientIdPrefix-${numInstances.incrementAndGet()}"
        }
    }

    @Volatile
    private var state = ProducerContainer.State.CREATED

    private var transactionId: String? = null
    private lateinit var clientId: String
    private lateinit var producer: Producer<K, V>

    override fun send(
        pairs: Collection<Pair<K, V>>,
        topic: String?,
        partition: Int?,
        timestamp: Instant?,
        onSuccess: OnSendSuccessCallback<K, V>?,
        onError: OnSendErrorCallback<K, V>?
    ): Future<List<SendResult<K?, V?>>> {
        val futures: List<CompletableFuture<SendResult<K?, V?>>> = pairs.map {
            send(it.first, it.second, topic, partition, timestamp, onSuccess, onError) as CompletableFuture
        }
        return  CompletableFuture.allOf(*futures.toTypedArray()).thenApply { futures.map { it.join() }.toList() }
    }

    override fun send(
        record: KafkaRecord<K?, V?>,
        onSuccess: OnSendSuccessCallback<K, V>?,
        onError: OnSendErrorCallback<K, V>?
    ): Future<SendResult<K?, V?>> {
        val producerRecord = KafkaRecord<K?, V?>(
            record.key,
            record.value,
            record.topic?:defaultTopic,
            record.partition,
            record.timestamp
        ).toProducerRecord()
        return send(producerRecord, onSuccess, onError)
    }

    override fun send(
        record: ProducerRecord<K?, V?>,
        onSuccess: OnSendSuccessCallback<K, V>?,
        onError: OnSendErrorCallback<K, V>?
    ) : CompletableFuture<SendResult<K?, V?>> {
        return runOrThrowIfIllegalState {
            val future = CompletableFuture<SendResult<K?, V?>>()
            logWithProducerInfo(Level.DEBUG, "Sending record $record")
            producer.send(record) { metadata: RecordMetadata, exception: Exception? ->

                if (exception != null) {
                    future.completeExceptionally(exception)

                    (onError?.let { DelegateSendCallback(onError = onError) }?: onSendCallback)
                        .onSendError(this, record, exception)
                } else {
                    future.complete(SendResult(record, metadata))

                    (onSuccess?.let { DelegateSendCallback(onSuccess = onSuccess) }?: onSendCallback)
                        .onSendSuccess(this, record, metadata)
                }
            }
            future
        }
    }

    override fun <T> execute(action: (producer: Producer<K, V>) -> T) = run { action(producer)  }

    override fun runTx(action: (ProducerContainer<K, V>) -> Unit): TransactionResult {
        return runOrThrowIfIllegalState {
            try {
                producer.beginTransaction()
                action.invoke(this)
                producer.commitTransaction()
                CommittedTransactionResult
            } catch (e: Exception) {
                when (e) {
                    is ProducerFencedException,
                    is OutOfOrderSequenceException,
                    is AuthorizationException -> {
                        logWithProducerInfo(
                            Level.ERROR,
                            "Unrecoverable error happened while executing producer transactional action. Close producer immediately",
                            e
                        )
                        close()
                        UnrecoverableErrorTransactionResult(e)
                    }
                    else -> {
                        logWithProducerInfo(
                            Level.ERROR,
                            "Error happened while executing producer transactional action. Abort current transaction",
                            e
                        )
                        producer.abortTransaction()
                        AbortedTransactionResult(e)
                    }
                }
                AbortedTransactionResult(e)
            }
        }
    }

    override fun init() {
        if (isInitialized()) {
            throw IllegalStateException("Producer is already initialized")
        }
        val producerConfigs = HashMap(configs.asMap())
        clientId = computeNextClientId(producerConfigs)
        transactionId = producerConfigs[ProducerConfig.TRANSACTIONAL_ID_CONFIG]?.toString()
        logWithProducerInfo(Level.INFO, "Initializing")
        producer = producerFactory?.make(producerConfigs, keySerializer, valueSerializer) ?: KafkaProducer(producerConfigs, keySerializer, valueSerializer)
        if (ProducerConfig.TRANSACTIONAL_ID_CONFIG in producerConfigs)
            producer.initTransactions()
        state = ProducerContainer.State.STARTED
    }

    override fun metrics(topic: String): Map<MetricName, Metric> {
        return runOrThrowIfIllegalState {
            producer.metrics()
        }
    }

    override fun partitionsFor(topic: String): List<PartitionInfo> {
        return runOrThrowIfIllegalState {
            producer.partitionsFor(topic)
        }
    }

    override fun flush() {
        runOrThrowIfIllegalState {
            logWithProducerInfo(Level.DEBUG, "Flushing")
            producer.flush()
        }
    }

    override fun close(timeout: Duration) {
        if (isClosed()) return // silently ignore call if producer is already closed.

        runOrThrowIfIllegalState {
            state = ProducerContainer.State.PENDING_SHUTDOWN
            logWithProducerInfo(Level.INFO, "Closing")
            producer.close(timeout)
            state = ProducerContainer.State.CLOSED
            logWithProducerInfo(Level.INFO, "Closed")
        }
    }

    private fun isClosed() =
        state == ProducerContainer.State.CLOSED ||
        state == ProducerContainer.State.PENDING_SHUTDOWN

    private fun isInitialized() = this::producer.isInitialized

    private fun <R> runOrThrowIfIllegalState(action: () -> R): R {
        if (!isInitialized()) throw IllegalStateException("Producer is not initialized yet")
        if (isClosed()) throw IllegalStateException("Cannot perform operation after producer has been closed")
        return action.invoke()
    }

    private fun logWithProducerInfo(level: Level, msg: String, exception: java.lang.Exception? = null) {
        val message = "Producer (clientId=$clientId${transactionId?.let {" , transactionalId=$transactionId" }?:""}): $msg"
        when(level) {
            Level.ERROR -> Log.error(message, exception)
            Level.WARN -> Log.warn(message)
            Level.INFO -> Log.info(message)
            Level.DEBUG -> Log.debug(message)
            else -> Log.debug(message)
        }
    }

    override fun state(): ProducerContainer.State = state

    data class Builder<K, V>(
        var configs: KafkaProducerConfigs,
        var keySerializer: Serializer<K> ?= null,
        var valueSerializer: Serializer<V> ?= null,
        var producerFactory: ProducerFactory? = null,
        var defaultTopic: String? = null,
        var onSendSuccess: OnSendSuccessCallback<K, V>? = null,
        var onSendError: OnSendErrorCallback<K, V>? = null,
        var onSendCallback: ProducerSendCallback<K, V>? = null
    ) : ProducerContainer.Builder<K, V> {

        override fun configure(init: KafkaProducerConfigs.() -> Unit) =
            apply {  configs.init() }

        override fun defaultTopic(topic: String) =
            apply { this.defaultTopic = topic }

        override fun producerFactory(producerFactory: ProducerFactory) =
            apply { this.producerFactory = producerFactory }

        override fun onSendError(callback: OnSendErrorCallback<K, V>) =
            apply { this.onSendError = callback }

        override fun onSendSuccess(callback: OnSendSuccessCallback<K, V>) =
            apply { this.onSendSuccess = callback }

        override fun onSendCallback(callback: ProducerSendCallback<K, V>) =
            apply { this.onSendCallback = callback }

        override fun keySerializer(serializer: Serializer<K>) =
            apply { this.keySerializer = serializer }

        override fun valueSerializer(serializer: Serializer<V>) =
            apply { this.valueSerializer = serializer }

        fun build(): ProducerContainer<K, V> = KafkaProducerContainer(
            configs,
            onSendCallback ?: DelegateSendCallback(onSendSuccess, onSendError),
            keySerializer,
            valueSerializer,
            producerFactory,
            defaultTopic
        )
    }

    private class DelegateSendCallback<K, V>(
        private val onSuccess: OnSendSuccessCallback<K, V>? = null,
        private val onError: OnSendErrorCallback<K, V>? = null
    ) : ProducerSendCallback<K, V> {

        override fun onSendError(
            container: ProducerContainer<K, V>,
            record: ProducerRecord<K?, V?>,
            error: Exception
        ) {
            this.onError?.invoke(container, record, error)
        }
        override fun onSendSuccess(
            container: ProducerContainer<K, V>,
            record: ProducerRecord<K?, V?>,
            metadata: RecordMetadata
        ) {
            onSuccess?.invoke(container, record, metadata)
        }
    }
}