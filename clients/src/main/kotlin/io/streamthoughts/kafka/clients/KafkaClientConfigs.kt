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

data class KafkaClientConfigs (
    var kafka : Kafka,
    var clientId: String? = null
) : Configure {

    private val configs = HashMap<String, Any?>()

    init {
        configs[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers.joinToString()
    }

    override fun with(key: String, value: Any?) {
        configs[key] = value
    }

    fun clientId(clientId: String) = apply { this.clientId = clientId }

    override fun asMap(): Map<String, Any?> {
        val configs = HashMap<String, Any?>(this.configs)
        clientId?.let { configs[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId }
        return configs
    }
}