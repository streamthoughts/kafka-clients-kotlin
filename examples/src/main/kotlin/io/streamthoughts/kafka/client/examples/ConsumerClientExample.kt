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

import io.streamthoughts.kafka.clients.consumer.KafkaConsumerConfigs
import io.streamthoughts.kafka.clients.loadConsumerConfigs
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import kotlin.system.exitProcess

fun main(args: Array<String>) {

    if (args.size != 2) {
        println("Missing required command line arguments: configFile topic")
        exitProcess(1)
    }

    val (config, topic) = args

    // Load properties from file and customize Consumer config.
    val configs: KafkaConsumerConfigs = loadConsumerConfigs(config)
        .keyDeserializer(StringDeserializer::class.java.name)
        .valueDeserializer(StringDeserializer::class.java.name)

    val consumer = KafkaConsumer<String, String>(configs)

    consumer.use {
        consumer.subscribe(listOf(topic))
        while(true) {
            consumer
                .poll(Duration.ofMillis(500))
                .forEach { record ->
                    println(
                        "Received record with key ${record.key()} " +
                        "and value ${record.value()} from topic ${record.topic()} and partition ${record.partition()}"
                    )
                }
        }
    }
}
