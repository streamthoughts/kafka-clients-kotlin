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
package io.streamthoughts.kafka.client.examples

import io.streamthoughts.kafka.clients.consumer.AutoOffsetReset
import io.streamthoughts.kafka.clients.consumer.ConsumerTask
import io.streamthoughts.kafka.clients.consumer.ConsumerWorker
import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandlers
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandlers
import io.streamthoughts.kafka.clients.consumer.listener.forEach
import io.streamthoughts.kafka.clients.kafka
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

fun main(args: Array<String>) {

    val consumerWorker: ConsumerWorker<String, String> = kafka("localhost:9092") {
        client {
            clientId("my-client")
        }

        val stringDeserializer: Deserializer<String> = StringDeserializer()
        consumer("my-group", stringDeserializer, stringDeserializer) {
            configure {
                pollRecordsMs(500)
                maxPollRecords(1000)
                autoOffsetReset(AutoOffsetReset.Earliest)
            }

            onDeserializationError(DeserializationErrorHandlers.silentlyReplaceWithNull())

            onConsumedError(ConsumedErrorHandlers.closeTaskOnConsumedError())

            onPartitionsAssigned { _: Consumer<*, *>, partitions ->
                println("Partitions assigned: $partitions")
            }

            onPartitionsRevokedAfterCommit { _: Consumer<*, *>, partitions ->
                println("Partitions revoked: $partitions")
            }

            onConsumed(forEach { _: ConsumerTask, value: String? ->
                println("consumed record-value: $value")
            })
        }
    }

    with (consumerWorker) {
        start("demo-topic", maxParallelHint = 4)
        runBlocking {
            println("All consumers started, waiting one minute before stopping")
            delay(Duration.ofMinutes(1).toMillis())
            stop()
        }
    }
}