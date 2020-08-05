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
package io.streamthoughts.kafka.clients.consumer

import io.streamthoughts.kafka.clients.consumer.KafkaConsumerWorker.KafkaConsumerWorker
import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandler
import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandlers.closeTaskOnConsumedError
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandler
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandlers
import io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener
import io.streamthoughts.kafka.clients.consumer.listener.noop
import io.streamthoughts.kafka.clients.loggerFor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

/**
 * [KafkaConsumerWorker] is the default [ConsumerWorker] implementation.
 */
class KafkaConsumerWorker<K, V> (
    private val configs: KafkaConsumerConfigs,
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val consumerRebalanceListener: ConsumerAwareRebalanceListener,
    private val batchRecordListener: ConsumerBatchRecordsListener<K, V>,
    private val onConsumedError: ConsumedErrorHandler,
    private val onDeserializationError: DeserializationErrorHandler<K, V>,
    private val consumerFactory: ConsumerFactory = ConsumerFactory.DefaultConsumerFactory
    ): ConsumerWorker<K, V> {

    companion object KafkaConsumerWorker {
        private val Log = loggerFor(KafkaConsumerWorker::class.java)
    }

    private val groupId: String = configs[ConsumerConfig.GROUP_ID_CONFIG].toString()

    private val defaultClientIdPrefix: String

    private var consumerTasks: Array<KafkaConsumerTask<K, V>> = emptyArray()

    private var consumerJobs: List<Job> = mutableListOf()

    private var isRunning = AtomicBoolean(false)

    init {
        defaultClientIdPrefix= "consumer-$groupId"
    }

    override fun groupId(): String {
        return groupId
    }

    @Synchronized
    override fun start(topic: String, maxParallelHint: Int) {
        start(getTopicSubscription(topic), maxParallelHint)
    }

    @Synchronized
    override fun start(topics: List<String>, maxParallelHint: Int) {
        start(getTopicSubscription(topics), maxParallelHint)
    }

    @Synchronized
    override fun start(pattern: Pattern, maxParallelHint: Int) {
       start(getTopicSubscription(pattern), maxParallelHint)
    }

    @Synchronized
    private fun start(subscription: TopicSubscription, maxParallelHint: Int) {
        Log.info("KafkaConsumerWorker(group: $groupId): Initializing io.streamthoughts.kafka.clients.consumer tasks ($maxParallelHint)")
        consumerTasks = Array(maxParallelHint) { taskId ->
            KafkaConsumerTask(
                consumerFactory,
                configs,
                subscription,
                keyDeserializer,
                valueDeserializer,
                batchRecordListener,
                clientId = computeClientId(taskId),
                consumerAwareRebalanceListener = consumerRebalanceListener,
                deserializationErrorHandler = onDeserializationError,
                consumedErrorHandler = onConsumedError
            )
        }
        doStart()
        isRunning.set(true)
    }

    private fun computeClientId(taskId: Int): String {
        val clientId = configs[CommonClientConfigs.CLIENT_ID_CONFIG]
        return clientId?.let { "$defaultClientIdPrefix-$taskId" } ?: ""
    }

    private fun doStart() {
        val threadNumber = AtomicInteger(1)
        val executor: ExecutorService = Executors.newFixedThreadPool(consumerTasks.size) {
            Thread(it, "io.streamthoughts.kafka.clients.consumer-$groupId-${threadNumber.getAndIncrement()}").also { t -> t.isDaemon = true }
        }
        val dispatcher: ExecutorCoroutineDispatcher = executor.asCoroutineDispatcher()
        val scope = CoroutineScope(dispatcher)

        consumerJobs = consumerTasks.map { task ->
            scope.launch {
                task.run()
            }
        }
    }

    override suspend fun joinAll() {
        consumerJobs.joinAll()
    }

    override fun close() {
        if (isRunning.get()) {
            Log.info("KafkaConsumerWorker(group: $groupId): Stopping all io.streamthoughts.kafka.clients.consumer tasks")
            consumerTasks.forEach { it.close() }
            isRunning.set(false)
        }
    }

    @Synchronized
    override fun pause() {
        Log.info("KafkaConsumerWorker(group: $groupId): Pausing all io.streamthoughts.kafka.clients.consumer tasks")
        consumerTasks.forEach { it.pause() }
    }

    @Synchronized
    override fun resume() {
        Log.info("KafkaConsumerWorker(group: $groupId): Resuming all io.streamthoughts.kafka.clients.consumer tasks")
        consumerTasks.forEach { it.resume() }
    }

    data class Builder<K, V>(
        var configs: KafkaConsumerConfigs,
        var keyDeserializer: Deserializer<K>,
        var valueDeserializer: Deserializer<V>,
        var onPartitionsAssigned: RebalanceListener? = null,
        var onPartitionsRevokedBeforeCommit: RebalanceListener? = null,
        var onPartitionsRevokedAfterCommit: RebalanceListener? = null,
        var onPartitionsLost: RebalanceListener? = null,
        var batchRecordListener: ConsumerBatchRecordsListener<K, V>? = null,
        var onDeserializationError: DeserializationErrorHandler<K, V>? = null,
        var consumerFactory: ConsumerFactory? = null,
        var onConsumedError: ConsumedErrorHandler? = null
    ) : ConsumerWorker.Builder<K, V> {

        override fun configure(init: KafkaConsumerConfigs.() -> Unit) {
            configs.init()
        }

        override fun factory(consumerFactory : ConsumerFactory) =
            apply { this.consumerFactory = consumerFactory }

        override fun onPartitionsAssigned(listener : RebalanceListener) =
            apply { this.onPartitionsAssigned = listener }

        override fun onPartitionsRevokedBeforeCommit(listener : RebalanceListener) =
            apply { this.onPartitionsRevokedAfterCommit = listener }

        override fun onPartitionsRevokedAfterCommit(listener : RebalanceListener) =
            apply { this.onPartitionsRevokedAfterCommit = listener }

        override fun onPartitionsLost(listener : RebalanceListener) =
            apply { this.onPartitionsLost = listener }

        override fun onDeserializationError(handler : DeserializationErrorHandler<K, V>)  =
            apply { onDeserializationError = handler }

        override fun onConsumedError(handler : ConsumedErrorHandler)  =
            apply { onConsumedError = handler }

        override fun onConsumed(listener: ConsumerBatchRecordsListener<K, V>) =
            apply { this.batchRecordListener = listener }

        override fun build(): ConsumerWorker<K, V>  =
            KafkaConsumerWorker(
                configs,
                keyDeserializer,
                valueDeserializer,
                SimpleConsumerAwareRebalanceListener(),
                batchRecordListener ?: noop(),
                onConsumedError ?: closeTaskOnConsumedError(),
                onDeserializationError ?: DeserializationErrorHandlers.logAndFail(),
                consumerFactory ?: ConsumerFactory.DefaultConsumerFactory
            )

        inner class SimpleConsumerAwareRebalanceListener: ConsumerAwareRebalanceListener {
            override fun onPartitionsRevokedBeforeCommit(consumer: Consumer<*, *>,
                                                         partitions: Collection<TopicPartition>) {
                doInvoke(onPartitionsRevokedBeforeCommit, consumer, partitions)
            }

            override fun onPartitionsRevokedAfterCommit(consumer: Consumer<*, *>,
                                                        partitions: Collection<TopicPartition>) {
                doInvoke(onPartitionsRevokedAfterCommit, consumer, partitions)
            }

            override fun onPartitionsAssigned(consumer: Consumer<*, *>,
                                              partitions: Collection<TopicPartition>) {
                doInvoke(onPartitionsAssigned, consumer, partitions)
            }

            override fun onPartitionsLost(consumer: Consumer<*, *>,
                                          partitions: Collection<TopicPartition>) {
                doInvoke(onPartitionsLost, consumer, partitions)
            }

            private fun doInvoke(listener: RebalanceListener?,
                                 consumer: Consumer<*, *>,
                                 partitions: Collection<TopicPartition>) {
                listener?.invoke(consumer, partitions)
            }
        }
    }
}