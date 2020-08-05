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

import kotlin.collections.HashMap


/**
 * The base class for client configuration.
 *
 * @see io.streamthoughts.kafka.clients.KafkaClientConfigs
 * @see io.streamthoughts.kafka.clients.consumer.KafkaConsumerConfigs
 * @see io.streamthoughts.kafka.clients.producer.KafkaProducerConfigs
 */
open class Configs protected constructor(props: Map<String, Any?> = emptyMap()) : MutableMap<String, Any?> {

    private val props = HashMap(props)

    override val entries: MutableSet<MutableMap.MutableEntry<String, Any?>>
        get() = props.entries

    override val keys: MutableSet<String>
        get() = props.keys

    override val size: Int
        get() = props.size

    override val values: MutableCollection<Any?>
        get() = props.values

    override fun containsKey(key: String): Boolean {
        return props.containsKey(key)
    }

    override fun containsValue(value: Any?): Boolean {
        return props.containsValue(value)
    }

    override fun get(key: String): Any? {
        return props[key]
    }

    override fun isEmpty(): Boolean {
        return props.isEmpty()
    }

    open fun with(key: String, value: Any?) = apply { this[key] = value }

    operator fun set(key: String, value: Any?) {
        props[key] = value
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Configs) return false

        if (props != other.props) return false

        return true
    }

    override fun hashCode(): Int {
        return props.hashCode()
    }

    override fun toString(): String {
        return "Configs[$props]"
    }

    override fun clear() {
        props.clear()
    }

    override fun put(key: String, value: Any?): Any? = props.put(key, value)

    override fun putAll(from: Map<out String, Any?>) = props.putAll(from)

    override fun remove(key: String): Any?  = props.remove(key)
}