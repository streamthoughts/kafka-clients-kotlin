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
package io.streamthoughts.kafka.tests

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.SystemTime
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.util.*
import kotlin.collections.HashMap

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker.
 */
class TestingEmbeddedKafka(config: Properties = Properties()) {

    companion object {
        val Log: Logger = LoggerFactory.getLogger(TestingEmbeddedKafka::class.java)

        private fun getTopicNames(adminClient: Admin): MutableSet<String> {
            return try {
                adminClient.listTopics().names().get()
            } catch (e: Exception) {
                throw RuntimeException("Failed to get topic names", e)
            }
        }
    }

    private val config: MutableMap<Any, Any> = HashMap(config)

    private lateinit var kafka: KafkaServer

    /**
     * @param securityProtocol the security protocol the returned broker list should use.
     *
     */
    fun bootstrapServers(securityProtocol: SecurityProtocol? = null): String {
        val port = if (securityProtocol == null) {
            val listenerName = kafka.config().advertisedListeners().apply(0).listenerName()
            kafka.boundPort(listenerName)
        }
        else {
            kafka.boundPort(ListenerName(securityProtocol.toString()))
        }
        return "${kafka.config().hostName()}:$port"
    }

    /**
     * Creates and starts an embedded Kafka broker.
     */
    fun start(overrides: Map<Any, Any> = emptyMap()) {
        config.putAll(overrides)
        config.putIfAbsent(KafkaConfig.LogDirProp(), "/tmp/kafka-logs")
        config.putIfAbsent(KafkaConfig.DeleteTopicEnableProp(), true)
        config.putIfAbsent(KafkaConfig.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L)
        config.putIfAbsent(KafkaConfig.GroupMinSessionTimeoutMsProp(), 0)
        config.putIfAbsent(KafkaConfig.GroupInitialRebalanceDelayMsProp(), 0)
        config.putIfAbsent(KafkaConfig.OffsetsTopicReplicationFactorProp(), 1.toShort())
        config.putIfAbsent(KafkaConfig.OffsetsTopicPartitionsProp(), 5)
        config.putIfAbsent(KafkaConfig.TransactionsTopicPartitionsProp(), 5)
        config.putIfAbsent(KafkaConfig.AutoCreateTopicsEnableProp(), true)
        val kafkaConfig = KafkaConfig(config, true)
        Log.debug(
            "Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
            logDir(), zookeeperConnect()
        )
        kafka = TestUtils.createServer(kafkaConfig, SystemTime())
        Log.debug(
            "Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
            bootstrapServers(), zookeeperConnect()
        )
    }

    /**
     * Stops the embedded broker and cleanup local logs directory.
     */
    fun stop() {
        Log.debug(
            "Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
            bootstrapServers(), zookeeperConnect()
        )
        kafka.shutdown()
        kafka.awaitShutdown()
        clearLogsDir()
        Log.debug(
            "Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
            bootstrapServers(), zookeeperConnect()
        )
    }

    private fun clearLogsDir() {
        Log.debug("Deleting logs.dir at {} ...", logDir())
        Files.walk(Paths.get(logDir()))
            .sorted(Comparator.reverseOrder())
            .forEach {
                try {
                    Files.delete(it)
                } catch (e: IOException) {
                    Log.error("Failed to delete entry in log dir {}", logDir(), e)
                }
            }
    }

    /**
     * Creates a Kafka [topic] with the given [partitions] number, [replication] factor and [config].
     */
    @JvmOverloads
    fun createTopic(
        topic: String,
        partitions: Int = 1,
        replication: Int = 1,
        config: Map<String?, String?>? = emptyMap()
    ) {
        Log.debug(
            "Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
            topic, partitions, replication, config
        )

        adminClient().use {adminClient ->
            val newTopic = NewTopic(topic, partitions, replication.toShort())
            newTopic.configs(config)
            try {
                adminClient.createTopics(listOf(newTopic)).all().get()
            } catch (e: Exception) {
                throw RuntimeException("Failed to create topic:$topic", e)
            }
        }
    }

    /**
     * @return the list of topics that exists on the embedded cluster.
     */
    fun topics(): Set<String> = adminClient().use { adminClient -> return getTopicNames(adminClient) }

    /**
     * Creates a new admin client.
     *
     * @return a new [org.apache.kafka.clients.admin.AdminClient] instance.
     */
    fun adminClient() =
        AdminClient.create(mutableMapOf(
            Pair(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()),
            Pair(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000)
        ))

    /**
     * Creates a new producer client.
     *
     * @return a new [org.apache.kafka.clients.producer.KafkaProducer] instance.
     */
    fun producerClient(config: Map<String, Any?> = emptyMap()): Producer<Any, Any> {
        val configs = HashMap(config)
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers()
        return KafkaProducer(configs)
    }

    /**
     * Creates a new consumer client.
     *
     * @return a new [org.apache.kafka.clients.consumer.KafkaConsumer] instance.
     */
    fun <K, V>  consumerClient(config: Map<String, Any?> = emptyMap(),
                               keyDeserializer: Deserializer<K>? = null,
                               valueDeserializer: Deserializer<V>? = null): Consumer<K, V> {
        val configs = HashMap(config)
        configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers()
        configs.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
        configs.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
        configs.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        configs.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        configs.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        return KafkaConsumer(configs, keyDeserializer, valueDeserializer)
    }

    fun <K, V> consumeUntilMinRecordsOrTimeout(
        topic: String,
        timeout: Duration = Duration.ofMinutes(1),
        expectedNumRecords: Int = Int.MAX_VALUE,
        keyDeserializer: Deserializer<K>? = null,
        valueDeserializer: Deserializer<V>? = null,
        consumerConfig: Map<String, Any?> = emptyMap()): List<ConsumerRecord<K, V>> {

         val consumer = consumerClient(consumerConfig, keyDeserializer, valueDeserializer)
         consumer.subscribe(listOf(topic))
         val records : MutableList<ConsumerRecord<K, V>> = mutableListOf()

         val begin = System.currentTimeMillis()
         while ( (System.currentTimeMillis() - begin) < timeout.toMillis() && records.size < expectedNumRecords) {
             consumer.poll(Duration.ofMillis(100)).forEach { records.add(it) }
         }
        return records

    }

    /**
     * Deletes the given [topics] from the cluster.
     */
    fun deleteTopics(topics: Collection<String?>) {
        try {
            adminClient().use { adminClient ->
                adminClient.deleteTopics(topics).all().get()
                val remaining: MutableSet<String?> = topics.toMutableSet()
                while (remaining.isNotEmpty()) {
                    val topicNames: Set<String> = adminClient.listTopics().names().get()
                    remaining.retainAll(topicNames)
                }
            }
        } catch (e: Exception) {
            throw RuntimeException("Failed to delete topics: $topics", e)
        }
    }

    private fun zookeeperConnect(): String {
        return config[KafkaConfig.ZkConnectProp()].toString()
    }

    private fun logDir(): String {
        return config[KafkaConfig.LogDirProp()].toString()
    }
}