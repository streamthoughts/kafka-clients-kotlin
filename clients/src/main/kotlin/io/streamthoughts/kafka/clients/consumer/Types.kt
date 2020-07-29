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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

typealias ConsumerBatchRecordListener<K, V> = (Consumer<*, *>, ConsumerRecords<K?, V?>) -> Unit
typealias ConsumerRecordListener<K, V> = (Consumer<*, *>, ConsumerRecord<K?, V?>) -> Unit
typealias ConsumerValueRecordWithKeyListener<K, V> = (Consumer<*, *>, Pair<K?, V?>) -> Unit
typealias ConsumerValueRecordListener<V> = (Consumer<*, *>, V?) -> Unit
typealias RebalanceListener = (Consumer<*, *>, Collection<TopicPartition> ) -> Unit