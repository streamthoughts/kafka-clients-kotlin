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

import io.streamthoughts.kafka.clients.kafka
import io.streamthoughts.kafka.clients.producer.Acks
import io.streamthoughts.kafka.clients.producer.ProducerContainer
import io.streamthoughts.kafka.clients.producer.callback.closeOnErrorProducerSendCallback
import org.apache.kafka.common.serialization.StringSerializer

fun main(args: Array<String>) {

    val producer: ProducerContainer<String, String> = kafka("localhost:9092") {
        client {
            clientId("my-client")
        }

        producer {
            configure {
                acks(Acks.InSyncReplicas)
            }
            keySerializer(StringSerializer())
            valueSerializer(StringSerializer())

            defaultTopic("demo-topic")

            onSendError(closeOnErrorProducerSendCallback())

            onSendSuccess{ _, _, metadata ->
                println("Record was sent successfully: topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()} ")
            }
        }
    }

    val messages = listOf("I ❤️ Logs", "Making Sense of Stream Processing", "Apache Kafka")
    producer.use {
        producer.init() // create internal KafkaProducer and eventually call initTransaction if transactional.id is set
        messages.forEach {
            producer.send(value = it)
        }
    }
}