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

import io.streamthoughts.kafka.clients.KafkaClientConfigs
import io.streamthoughts.kafka.clients.consumer.KafkaConsumerWorker.KafkaConsumerWorker
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandler
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandlers
import io.streamthoughts.kafka.clients.loggerFor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.Consumer
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
class KafkaConsumerWorker<K, V>(
    clientConfigure: KafkaClientConfigs,
    private val groupId: String,
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private var onPartitionsAssigned: RebalanceListener? = null,
    private var onPartitionsRevokedBeforeCommit: RebalanceListener? = null,
    private var onPartitionsRevokedAfterCommit: RebalanceListener? = null,
    private var onPartitionsLost: RebalanceListener? = null,
    private var batchRecordListener: ConsumerBatchRecordListener<K, V> = { _, _ -> Unit},
    private var onDeserializationError: DeserializationErrorHandler<K, V> = DeserializationErrorHandlers.logAndFail(),
    private var consumerFactory: ConsumerFactory = ConsumerFactory.DefaultConsumerFactory
    ): ConsumerWorker<K, V> {

    companion object KafkaConsumerWorker {
        private val Log = loggerFor(KafkaConsumerWorker::class.java)
    }

    private val defaultClientIdPrefix = "io.streamthoughts.kafka.clients.consumer-$groupId"

    private val consumerConfigs: KafkaConsumerConfigs = KafkaConsumerConfigs(clientConfigure, groupId = groupId)

    private var consumerTasks: Array<ConsumerTask<K, V>> = emptyArray()

    private var consumerJobs: List<Job> = mutableListOf()

    private var isRunning = AtomicBoolean(false)

    override fun groupId(): String {
        return groupId
    }

    override fun configure(init: KafkaConsumerConfigs.() -> Unit) {
        consumerConfigs.init()
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

    override fun onConsumed(listener: ConsumerBatchRecordListener<K, V>) =
        apply { this.batchRecordListener = listener }

    @JvmName("onConsumedRecord")
    fun onConsumed(listener: ConsumerRecordListener<K, V>) =
        apply { this.batchRecordListener = { c, records -> records.forEach { listener.invoke(c, it) } } }

    @JvmName("onConsumedValueRecordWithKey")
    fun onConsumed(listener: ConsumerValueRecordWithKeyListener<K, V>) =
        apply { this.batchRecordListener = { c, records -> records.forEach { listener.invoke(c, Pair(it.key(), it.value())) } } }

    @JvmName("onConsumedValueRecord")
    fun onConsumed(listener: ConsumerValueRecordListener<V>) =
        apply { this.batchRecordListener = { c, records -> records.forEach { listener.invoke(c, it.value()) } } }

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
            ConsumerTask(
                consumerFactory,
                consumerConfigs,
                subscription,
                keyDeserializer,
                valueDeserializer,
                batchRecordListener,
                clientId = computeClientId(taskId),
                consumerAwareRebalanceListener = SimpleConsumerAwareRebalanceListener(),
                deserializationErrorHandler = onDeserializationError
            )

        }
        doStart()
        isRunning.set(true)
    }

    private fun computeClientId(taskId: Int): String {
        val clientId = consumerConfigs.clientConfigs.clientId
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

    override fun stop() {
        if (isRunning.get()) {
            Log.info("KafkaConsumerWorker(group: $groupId): Stopping all io.streamthoughts.kafka.clients.consumer tasks")
            consumerTasks.forEach { it.shutdown() }
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