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
import io.streamthoughts.kafka.clients.KafkaClientConfigs
import io.streamthoughts.kafka.clients.KafkaRecord
import io.streamthoughts.kafka.clients.loggerFor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.Logger
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger


class KafkaProducerContainer<K, V>(clientConfigure: KafkaClientConfigs,
                                   private var keySerializer: Serializer<K> ?= null,
                                   private var valueSerializer: Serializer<V> ?= null,
                                   private var producerFactory: ProducerFactory? = null,
                                   private var defaultTopic: String? = null,
                                   private var defaultOnSendError: OnSendErrorCallback<K, V>? = null,
                                   private var defaultOnSendSuccess: OnSendSuccessCallback<K, V>? = null
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

    private val configs: KafkaProducerConfigs = KafkaProducerConfigs(clientConfigure)

    private var transactionId: String? = null
    private lateinit var clientId: String
    private lateinit var producer: Producer<K, V>

    override fun configure(init: KafkaProducerConfigs.() -> Unit) =
        apply {  configs.init() }

    override fun defaultTopic(topic: String) =
        apply { this.defaultTopic = topic }

    override fun onSendError(callback: OnSendErrorCallback<K, V>) =
        apply { this.defaultOnSendError = callback }

    override fun onSendSuccess(callback: OnSendSuccessCallback<K, V>) =
        apply { this.defaultOnSendSuccess = callback }

    override fun keySerializer(serializer: Serializer<K>) =
        apply { this.keySerializer = serializer }

    override fun valueSerializer(serializer: Serializer<V>) =
        apply { this.valueSerializer = serializer }


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
        return runIfInitializedOrThrowIllegal {
            val future = CompletableFuture<SendResult<K?, V?>>()
            logWithProducerInfo(Level.DEBUG, "Sending record $record")
            producer.send(record) { metadata: RecordMetadata, exception: Exception? ->
                if (exception != null) {
                    future.completeExceptionally(exception)
                    (onError ?: defaultOnSendError)?.invoke(producer, record, exception)
                } else {
                    future.complete(SendResult(record, metadata))
                    (onSuccess ?: defaultOnSendSuccess)?.invoke(producer, record, metadata)
                }
            }
            future
        }
    }

    override fun <T> execute(action: (producer: Producer<K, V>) -> T) = run { action(producer)  }

    override fun runTx(action: (ProducerContainer<K, V>) -> Unit): TransactionResult {
        return runIfInitializedOrThrowIllegal {
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
    }

    override fun metrics(topic: String): Map<MetricName, Metric> {
        return runIfInitializedOrThrowIllegal {
            producer.metrics()
        }
    }

    override fun partitionsFor(topic: String): List<PartitionInfo> {
        return runIfInitializedOrThrowIllegal {
            producer.partitionsFor(topic)
        }
    }

    override fun flush() {
        runIfInitializedOrThrowIllegal {
            logWithProducerInfo(Level.DEBUG, "Flushing")
            producer.flush()
        }
    }

    override fun close() {
        runIfInitializedOrThrowIllegal {
            logWithProducerInfo(Level.INFO, "Closing")
            producer.close()
            logWithProducerInfo(Level.INFO, "Closed")
        }
    }

    private fun isInitialized() = this::producer.isInitialized

    private fun <R> runIfInitializedOrThrowIllegal(action: () -> R): R {
        if (!isInitialized())
            throw IllegalStateException("Producer is not initialized yet.")
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
}