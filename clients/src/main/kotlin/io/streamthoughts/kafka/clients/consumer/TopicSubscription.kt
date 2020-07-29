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

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import java.util.regex.Pattern

fun getTopicSubscription(pattern: Pattern): TopicSubscription = PatternTopicsSubscription(pattern)
fun getTopicSubscription(topic: String): TopicSubscription = ListTopicsSubscription(listOf(topic))
fun getTopicSubscription(topics: List<String>): TopicSubscription = ListTopicsSubscription(topics)

/**
 * The default interface to wrap a [org.apache.kafka.clients.consumer.Consumer] subscription.
 */
interface TopicSubscription {

    /**
     * @see [Consumer.subscribe]
     */
    fun subscribe(consumer: Consumer<*, *>, consumerRebalanceListener: ConsumerRebalanceListener)
}

/**
 * A topic list subscription.
 */
private class ListTopicsSubscription(private val topics: List<String>): TopicSubscription {

    override fun subscribe(consumer: Consumer<*, *>, consumerRebalanceListener: ConsumerRebalanceListener) {
        consumer.subscribe(topics, consumerRebalanceListener)
    }

    override fun toString(): String {
        return "Subscription(topics=$topics)"
    }
}

/**
 * A topic pattern subscription.
 */
private class PatternTopicsSubscription(private val pattern: Pattern): TopicSubscription {

    override fun subscribe(consumer: Consumer<*, *>, consumerRebalanceListener: ConsumerRebalanceListener) {
        consumer.subscribe(pattern, consumerRebalanceListener)
    }

    override fun toString(): String {
        return "Subscription(pattern=$pattern)"
    }

}