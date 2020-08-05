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
package io.streamthoughts.kafka.clients

import org.apache.kafka.clients.CommonClientConfigs
import java.io.InputStream
import java.util.Properties
import kotlin.collections.HashMap
import kotlin.collections.Map
import kotlin.collections.emptyMap
import kotlin.collections.joinToString
import kotlin.collections.mutableMapOf


open class KafkaClientConfigs constructor(props: Map<String, Any?> = emptyMap()): Configs(props) {

    constructor(kafka : Kafka): this(bootstrapServersConfig(kafka))

    companion object {
        private fun bootstrapServersConfig(kafka: Kafka) = mutableMapOf<String, Any?>(
            Pair(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers.joinToString())
        )
    }

    /**
     * @see [CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG]
     */
    fun bootstrapServers(bootstrapServers: Array<String>) =
        apply { this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers.joinToString() }

    /**
     * @see [CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG]
     */
    fun bootstrapServers(bootstrapServers: String) =
        apply { this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers }

    /**
     * @see [CommonClientConfigs.CLIENT_ID_CONFIG]
     */
    fun clientId(clientId: String) =
        apply { this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId }

    override fun with(key: String, value: Any?) = apply { super.with(key, value) }
}

/**
 * Convenient method to create and populate a new [KafkaClientConfigs] from a [configFile].
 */
fun loadClientConfigs(configFile: String): KafkaClientConfigs = KafkaClientConfigs().load(configFile)

/**
 * Convenient method to create and populate a new [KafkaClientConfigs] from an [inputStream].
 */
fun loadClientConfigs(inputStream: InputStream): KafkaClientConfigs = KafkaClientConfigs().load(inputStream)

/**
 * Creates a new [KafkaClientConfigs] with no properties.
 */
fun emptyClientConfigs(): KafkaClientConfigs = KafkaClientConfigs()

/**
 * Creates a new [KafkaClientConfigs] with the given [props].
 */
fun clientConfigsOf(props: Map<String, Any?>): KafkaClientConfigs = KafkaClientConfigs(HashMap(props))

/**
 * Creates a new [KafkaClientConfigs] with the given [props].
 */
fun clientConfigsOf(props: Properties): KafkaClientConfigs = clientConfigsOf(props.toStringMap())