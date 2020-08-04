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
package io.streamthoughts.kafka.clients.consumer.error

import io.streamthoughts.kafka.clients.consumer.ConsumerTask
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Handles errors thrown during the processing of a non-empty batch of [ConsumerRecord]
 * using a given [io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener]
 */
interface ConsumedErrorHandler {

    /**
     * This method is invoked when an [thrownException] is thrown while a [consumerTask] is processing
     * a non-empty batch of [records].
     *
     * @param consumerTask    the [ConsumerTask] polling records.
     * @param records         the remaining [records] to be processed (including the one that failed).
     * @param thrownException the [Exception] that was thrown while processing [records].
     */
    fun handle(consumerTask: ConsumerTask, records: List<ConsumerRecord<*, *>>, thrownException: Exception)
}