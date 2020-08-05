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
package io.streamthoughts.kafka.clients.consumer.error

import io.streamthoughts.kafka.clients.consumer.ConsumerTask
import io.streamthoughts.kafka.clients.loggerFor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import java.time.Duration
import kotlin.math.max


object ConsumedErrorHandlers {
    fun closeTaskOnConsumedError(): ConsumedErrorHandler = CloseTaskOnConsumedError
    fun logAndCommitOnConsumedError(): ConsumedErrorHandler = LogAndCommitOnConsumedError
}

/**
 * Stops the [ConsumerTask] when an error is thrown while a non-empty batch of [ConsumerRecord] is being processed
 * by a [io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener].
 */
private object CloseTaskOnConsumedError: ConsumedErrorHandler {

    private val Log: Logger = loggerFor(CloseTaskOnConsumedError.javaClass)

    override fun handle(consumerTask: ConsumerTask, records: List<ConsumerRecord<*, *>>, thrownException: Exception) {
        Log.error("Stopping consumerTask after an exception was thrown while processing records", thrownException)
        consumerTask.close(Duration.ZERO)
    }
}

/**
 * Log and skips all records when an error is thrown while a non-empty batch of [ConsumerRecord] is being processed
 * by a [io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener].
 *
 * This [ConsumedErrorHandler] will commit the offsets for the failing records batch.
 */
private object LogAndCommitOnConsumedError: ConsumedErrorHandler {
    private val Log: Logger = loggerFor(LogAndCommitOnConsumedError.javaClass)

    override fun handle(consumerTask: ConsumerTask, records: List<ConsumerRecord<*, *>>, thrownException: Exception) {
        Log.error("Failed to process records: $records. Ignore and continue processing.", thrownException)
        // The ConsumerTask doesn't automatically commit consumer offsets after an exception is thrown.
        // Thus, we have to manually commit offsets to actually skip records.
        consumerTask.commitSync(offsetsToCommitFor(records))
    }

    private fun offsetsToCommitFor(records: List<ConsumerRecord<*, *>>): Map<TopicPartition, OffsetAndMetadata> {
        val offsetsToCommit: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
        records.forEach { r ->
            val partition = TopicPartition(r.topic(), r.partition())
            val current = offsetsToCommit[partition]?.offset() ?: 0
            offsetsToCommit[partition] =  OffsetAndMetadata(max(current, r.offset() + 1))
        }
        return offsetsToCommit
    }
}
