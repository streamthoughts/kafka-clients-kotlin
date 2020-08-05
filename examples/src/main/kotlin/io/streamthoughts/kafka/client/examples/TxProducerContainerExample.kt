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

import io.streamthoughts.kafka.clients.producer.Acks
import io.streamthoughts.kafka.clients.producer.KafkaProducerConfigs
import io.streamthoughts.kafka.clients.producer.KafkaProducerContainer
import io.streamthoughts.kafka.clients.producer.loadProducerConfigs
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.system.exitProcess


fun main(args: Array<String>) {

    if (args.size != 2) {
        println("Missing required command line arguments: configFile topic")
        exitProcess(1)
    }

    val (config, topic) = args


    // Load properties from file and customize Producer config.
    val configs: KafkaProducerConfigs = loadProducerConfigs(config)
        .acks(Acks.Leader)
        .keySerializer(StringSerializer::class.java.name)
        .valueSerializer(StringSerializer::class.java.name)

    val producer = KafkaProducerContainer.Builder<Any?, String>(configs)
        .defaultTopic(topic)
        .configure {
            transactionalId("my-tx-id")
            enableIdempotence(true)
        }
        .onSendSuccess { _, _, m ->
            println("Record was successfully sent (topic=${m.topic()}, partition=${m.partition()}, offset= ${m.offset()})")
        }
        .onSendError { _, _, e ->
            e.printStackTrace()
        }
        .build()

    val messages = listOf("I ❤️ Logs", "Making Sense of Stream Processing", "Apache Kafka")

    producer.use {
        producer.init()
        producer.runTx {
            messages.forEach{producer.send(value = it)}
        }
    }
}