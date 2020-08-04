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

import ch.qos.logback.classic.Level
import io.streamthoughts.kafka.clients.consumer.ConsumerTask.State
import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandler
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandler
import io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener
import io.streamthoughts.kafka.clients.loggerFor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import java.time.Duration
import java.util.LinkedList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.HashMap
import kotlin.math.max

class KafkaConsumerTask<K, V>(
    consumerFactory: ConsumerFactory,
    consumerConfigs: KafkaConsumerConfigs,
    private val subscription: TopicSubscription,
    private val keyDeserializer: Deserializer<K>,
    private val valueDeserializer: Deserializer<V>,
    private val listener: ConsumerBatchRecordsListener<K, V>,
    private var clientId: String = "",
    private val deserializationErrorHandler: DeserializationErrorHandler<K, V>,
    private val consumedErrorHandler: ConsumedErrorHandler? = null,
    private val consumerAwareRebalanceListener : ConsumerAwareRebalanceListener? = null
) : ConsumerTask {

    companion object {
        private val Log = loggerFor(KafkaConsumerTask::class.java)

        private fun <K, V> flatten(records: Map<TopicPartition, List<ConsumerRecord<K?, V?>>>): List<ConsumerRecord<K?, V?>> {
            return records.flatMap { (_, v) -> v }.toList()
        }
    }

    @Volatile
    private var state = State.CREATED

    private val consumer: Consumer<ByteArray, ByteArray>

    // create a new ConsumerConfig to get default config values
    private val consumerConfig: ConsumerConfig

    private val groupId: String

    private val pollTime = Duration.ofMillis(consumerConfigs.pollRecordsMs)

    private val isAutoCommitEnabled: Boolean

    private val isShutdown: AtomicBoolean = AtomicBoolean(false)

    private val shutdownLatch: CountDownLatch = CountDownLatch(1)

    private var assignedPartitions: MutableList<TopicPartition> = arrayListOf()

    private val consumedOffsets: MutableMap<TopicPartition, Long> = HashMap()

    init {
        val props= HashMap(consumerConfigs.asMap())

        if (clientId.isEmpty()) {
            clientId = props[ConsumerConfig.CLIENT_ID_CONFIG].toString()
        }

        props[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java

        // by default; disable auto-commit unless explicitly set in the configuration
        if (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG !in props) {
            props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        }

        consumer = consumerFactory.make(props)
        consumerConfig = object : ConsumerConfig(props, false) { }
        groupId = consumerConfig.getString(ConsumerConfig.GROUP_ID_CONFIG)
        clientId = consumerConfig.getString(ConsumerConfig.CLIENT_ID_CONFIG)
        isAutoCommitEnabled = consumerConfig.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
    }

    override fun pause() {
        val assignment = consumer.assignment()
        logWithConsumerInfo(Level.INFO, "Pausing consumption for: $assignment")
        state = State.PAUSED
        consumer.pause(assignment)
    }

    override fun resume() {
        val assignment = consumer.assignment()
        logWithConsumerInfo(Level.INFO, "Resuming consumption for: $assignment")
        state = State.RUNNING
        consumer.resume(assignment)
    }

    override fun state(): State = state

    override suspend fun run() {
        state = State.STARTING
        logWithConsumerInfo(Level.INFO, "Starting")
        subscribeConsumer()
        try {
            while (isStillRunning()) {
                isCancelled()
                pollOnce()
            }
        } catch (e: WakeupException) {
            if (!isShutdown.get()) throw e
            else {
                logWithConsumerInfo(Level.INFO, "Stop polling due to the consumer-task is being closed")
            }
        } catch (e: CancellationException) {
            logWithConsumerInfo(Level.INFO, "Stop polling due to the consumer-task has been canceled")
            throw e
        } finally {
            state = State.PENDING_SHUTDOWN
            consumer.close()
            state = State.SHUTDOWN
            logWithConsumerInfo(Level.INFO, "Closed")
            shutdownLatch.countDown()
        }
    }

    private fun isStillRunning() = !isShutdown.get()

    @Throws(CancellationException::class)
    private suspend fun isCancelled() {
        yield() // check if the Job of the current coroutine is cancelled before polling
    }

    private fun subscribeConsumer() {
        subscription.subscribe(consumer, rebalanceListener())
    }

    private fun pollOnce() {
        val rawRecords: ConsumerRecords<ByteArray, ByteArray> = consumer.poll(pollTime)

        if (state == State.PARTITIONS_ASSIGNED) {
            state = State.RUNNING
        }

        if (!rawRecords.isEmpty) {
            // deserialize all records using user-provided Deserializer
            val recordsPerPartitions: Map<TopicPartition, List<ConsumerRecord<K?, V?>>> =
                rawRecords.partitions()
                    .map { Pair(it, deserialize(rawRecords.records(it))) }
                    .toMap()
            try {
                processBatchRecords(ConsumerRecords(recordsPerPartitions))
                updateConsumedOffsets(rawRecords) // only update once all records from batch have been processed.
                mayCommitAfterBatch()
            } catch (e: Exception) {
                mayHandleConsumedError(recordsPerPartitions, e)
            }
        }
    }

    private fun mayHandleConsumedError(recordsPerPartitions: Map<TopicPartition, List<ConsumerRecord<K?, V?>>>,
                                       thrownException: Exception
    ) {
        consumedErrorHandler?.handle(
            this,
            flatten(recordsPerPartitions),
            thrownException
        )
    }

    private fun processBatchRecords(records: ConsumerRecords<K?, V?>) {
        listener.handle(this, records)
    }

    private fun updateConsumedOffsets(records: ConsumerRecords<*, *>) {
        records.partitions().forEach{topicPartition ->
            run {
                var position = consumedOffsets[topicPartition] ?: 0
                records.records(topicPartition).forEach {
                    position = max(position, it.offset())
                }
                consumedOffsets[topicPartition] = position
            }
        }
    }

    override fun shutdown() {
        logWithConsumerInfo(Level.INFO, "Closing")
        isShutdown.set(true)
        consumer.wakeup()
        shutdownLatch.await()
    }

    override fun shutdown(timeout: Duration) {
        logWithConsumerInfo(Level.INFO, "Closing")
        isShutdown.set(true)
        consumer.wakeup()
        if (timeout != Duration.ZERO) {
            try {
                shutdownLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                logWithConsumerInfo(Level.WARN, "Failed to close consumer before timeout")
            }
        }
    }

    private fun deserialize(records: List<ConsumerRecord<ByteArray, ByteArray>>): List<ConsumerRecord<K?, V?>> {
        val deserialized = LinkedList<ConsumerRecord<K?, V?>>()
        for (record : ConsumerRecord<ByteArray, ByteArray> in records) {

            val topic = record.topic()

            val pair: Pair<K?, V?>? = try {
                val key: K = keyDeserializer.deserialize(topic, record.headers(), record.key())
                val value: V = valueDeserializer.deserialize(topic, record.headers(), record.value())
                Pair(key, value)
            } catch (e: Exception) {
                when(val response = deserializationErrorHandler.handle(record, e)) {
                    is DeserializationErrorHandler.Response.Replace<K, V> -> Pair(response.key, response.value)
                    is DeserializationErrorHandler.Response.Fail<K, V> -> throw e
                    is DeserializationErrorHandler.Response.Skip<K, V> -> null
                    else -> throw e
                }
            }

            pair?.let {
                val cr = ConsumerRecord(
                    topic,
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.checksum() ,
                    record.serializedKeySize(),
                    record.serializedValueSize(),
                    pair.first,
                    pair.second,
                    record.headers())
                deserialized.add(cr)
            }
        }
        return deserialized
    }

    private fun rebalanceListener(): ConsumerRebalanceListener {
        return object: ConsumerRebalanceListener{
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
                logWithConsumerInfo(Level.INFO, "Partitions Assigned: $partitions")
                state = State.PARTITIONS_ASSIGNED
                assignedPartitions.addAll(partitions)
                consumerAwareRebalanceListener?.onPartitionsAssigned(consumer, partitions)
                if (!partitions.isEmpty()) mayCommitOnAssignment()
            }

            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
                logWithConsumerInfo(Level.INFO, "Partitions Revoked: $partitions")
                state = State.PARTITIONS_REVOKED
                consumerAwareRebalanceListener?.onPartitionsRevokedBeforeCommit(consumer, partitions)

                commitSync(offsetAndMetadataToCommit())

                consumerAwareRebalanceListener?.onPartitionsRevokedAfterCommit(consumer, partitions)
                assignedPartitions.clear()
            }

            override fun onPartitionsLost(partitions: MutableCollection<TopicPartition>) {
                logWithConsumerInfo(Level.INFO, "Partitions Lost: $partitions")
                consumerAwareRebalanceListener?.onPartitionsLost(consumer, partitions)
                assignedPartitions.clear()
            }
        }
    }

    private fun offsetAndMetadataToCommit() = consumedOffsets.map { Pair(it.key, OffsetAndMetadata(it.value + 1)) }.toMap()

    private fun mayCommitAfterBatch() {
        if (!isAutoCommitEnabled && consumedOffsets.isNotEmpty()) {
            commitAsync(offsetAndMetadataToCommit())
            consumedOffsets.clear()
        }
    }

    private fun mayCommitOnAssignment() {
        if (consumerConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) == AutoOffsetReset.Latest) {
            val positionsToCommit = assignedPartitions.map { topicPartition ->
                val offset = consumer.position(topicPartition)
                Pair(topicPartition, OffsetAndMetadata(offset))
            }.toMap()
            commitSync(positionsToCommit)
        }
    }

    override fun commitAsync(offsets: Map<TopicPartition, OffsetAndMetadata>?) {
        logWithConsumerInfo(Level.INFO, "Committing offsets async-synchronously for positions: $offsets")
        consumer.commitAsync(offsets) {
            _, exception -> if (exception != null)    {
                logWithConsumerInfo(Level.WARN, "Fail to commit position async-synchronously", exception)
            }
        }
    }

    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>?) {
        if (consumer.assignment().isEmpty()) return // no need to commit if no partition is assign to this consumer
        try {
            if (offsets == null) {
                logWithConsumerInfo(Level.WARN, "Committing offsets synchronously for consumed records")
                consumer.commitSync()
            } else {
                logWithConsumerInfo(Level.WARN, "Committing offsets synchronously for positions: $offsets")
                consumer.commitSync(offsets)
            }
            logWithConsumerInfo(Level.WARN, "Offsets committed for partitions: $assignedPartitions")
        } catch (e: RetriableCommitFailedException) {
            commitSync(offsets)
        } catch (e : RebalanceInProgressException) {
            logWithConsumerInfo(Level.WARN, "Error while committing offsets due to a rebalance in progress. Ignored")
        }
    }

    override fun toString(): String {
        return "ConsumerTask(groupId='$groupId', subscription=$subscription, assignedPartitions=$assignedPartitions, state=$state)"
    }

    private fun logWithConsumerInfo(level: Level, msg: String, exception: java.lang.Exception? = null) {
        val message = "Consumer(groupId=$groupId, clientId=$clientId): $msg"
        when(level) {
            Level.ERROR -> Log.error(message, exception)
            Level.WARN -> Log.warn(message)
            Level.INFO -> Log.info(message)
            Level.DEBUG -> Log.debug(message)
            else -> Log.debug(message)
        }
    }

    override fun <T> execute(action: (client: Consumer<ByteArray, ByteArray>) -> T): T  = run { action(consumer)  }
}