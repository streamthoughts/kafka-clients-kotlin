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
package io.streamthoughts.kafka.clients.producer.callback

import io.streamthoughts.kafka.clients.loggerFor
import io.streamthoughts.kafka.clients.producer.ProducerContainer
import io.streamthoughts.kafka.clients.producer.ProducerContainer.*
import io.streamthoughts.kafka.clients.producer.callback.ProducerSendCallback.CloseOnErrorProducerSendCallback
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import java.time.Duration

typealias OnSendErrorCallback<K, V> = (container: ProducerContainer<K, V>, record: ProducerRecord<K?, V?>, error: Exception) -> Unit
typealias OnSendSuccessCallback<K, V> = (container: ProducerContainer<K, V>, record: ProducerRecord<K?, V?>, metadata: RecordMetadata) -> Unit

/**
 * Creates a new a [CloseOnErrorProducerSendCallback]
 */
fun <K, V> closeOnErrorProducerSendCallback(then: OnSendErrorCallback<K, V>? = null): OnSendErrorCallback<K, V> =
    CloseOnErrorProducerSendCallback(then)::onSendError

interface ProducerSendCallback<K, V> {

    /**
     * This method is invoked when an error happen while sending a record.
     */
    fun onSendError(container: ProducerContainer<K, V>, record: ProducerRecord<K?, V?>, error: Exception) {}

    /**
     * This method is invoked when a record has been sent successfully.
     */
    fun onSendSuccess(container: ProducerContainer<K, V>, record: ProducerRecord<K?, V?>, metadata: RecordMetadata) {}

    /**
     * This [ProducerSendCallback] closes the [ProducerContainer] on the first exception that happens while
     * sending a record (i.e on the the first callback).
     *
     * Scenario :
     *
     * If leaders are unreachable, then record batches will expire after
     * [org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG] for all partitions for which
     * there is no in-flight request (retries are only applied for in-flight requests that fail)
     *
     * This may eventually lead to records re-ordering if the application continue to send new records even after
     * the [org.apache.kafka.clients.producer.Producer] expired buffered batches.
     * For this reason the producer client should be closed on the first callback.
     *
     * @constructor Creates a new [CloseOnErrorProducerSendCallback] that will optionally invoke the given [callback]
     *              on each record and after closing the container.
     */
    class CloseOnErrorProducerSendCallback<K, V>(
        private val callback : OnSendErrorCallback<K, V>? = null
    ) : ProducerSendCallback<K, V> {

        companion object {
            val Log: Logger = loggerFor(CloseOnErrorProducerSendCallback::class.java)
        }

        override fun onSendError(container: ProducerContainer<K, V>, record: ProducerRecord<K?, V?>, error: Exception) {
            val currentState = container.state()
            if (currentState != State.CLOSED && currentState != State.PENDING_SHUTDOWN) {
                Log.error("Closing producer due to an error while sending record to topic: ${record.topic()}", error)
                container.execute { producer -> {
                    producer.close(Duration.ZERO)
                } }
            }
            callback?.invoke(container, record, error)
        }
    }
}