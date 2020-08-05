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

import io.streamthoughts.kafka.clients.loadProducerConfigs
import io.streamthoughts.kafka.clients.producer.Acks
import io.streamthoughts.kafka.clients.producer.KafkaProducerConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
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

    val producer = KafkaProducer<String, String>(configs)

    val messages = listOf("I ❤️ Logs", "Making Sense of Stream Processing", "Apache Kafka")

    producer.use {
        messages.forEach {value ->
            val record = ProducerRecord<String, String>(topic, value)
            producer.send(record) { m: RecordMetadata, e: Exception? ->
                when (e) {
                    null -> println("Record was successfully sent (topic=${m.topic()}, partition=${m.partition()}, offset= ${m.offset()})")
                    else -> e.printStackTrace()
                }
            }
        }
    }
}
