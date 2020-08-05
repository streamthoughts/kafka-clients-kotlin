# Kafka Clients for Kotlin

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/LICENSE)

## Warning

Be aware that this package is still in heavy development. Some breaking change will occur in future weeks and months.
Thank's for your comprehension.

## What is Kafka Clients for Kotlin ?

The **Kafka Clients for Kotlin** projects packs with convenient Kotlin API for the development of Kafka-based event-driven applications.
It provides high-level abstractions both for sending records `ProducerContainer` and consuming records from topics using one or many
concurrent consumers `KafkaConsumerWorker`.

In addition, it provides builder classes to facilitate the configuration of `Producer` and `Consumer` objects: `KafkaProducerConfigs` and `KafkaConsumerConfigs`

**Kafka Clients for Kotlin** is based on the pure java `kafka-clients`.

## How to contribute ?

The project is in its early stages so it can be very easy to contribute by proposing APIs changes, new features and so on. 
Any feedback, bug reports and PRs are greatly appreciated!

* Source Code: https://github.com/streamthoughts/kafka-clients-kotlin
* Issue Tracker: https://github.com/streamthoughts/kafka-clients-kotlin/issues


## Show your support

You think this project can help you or your team to develop kafka-based application with Kotlin ?
Please ⭐ this repository to support us!

## How to give it a try ?

Just add **Kafka Clients for Kotlin** to the dependencies of your projects. 

### For Maven
```xml
<dependency>
  <groupId>io.streamthoughts</groupId>
  <artifactId>kafka-clients-kotlin</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Getting Started

### Writing messages to Kafka

**Example: How to create `KafkaProducer` config ?**

```kotlin
val configs = producerConfigsOf()
    .client { bootstrapServers("localhost:9092") }
    .acks(Acks.Leader)
    .keySerializer(StringSerializer::class.java.name)
    .valueSerializer(StringSerializer::class.java.name)
```

**Example with standard `KafkaProducer` (i.e : using java `kafka-clients`)**

```kotlin
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
```

N.B: See the full source code: [ProducerClientExample.kt](https://github.com/streamthoughts/kafka-clients-kotlin/blob/master/examples/src/main/kotlin/io/streamthoughts/kafka/client/examples/ProducerClientExample.kt)

**Example with Kotlin DSL**

```kotlin
val producer: ProducerContainer<String, String> = kafka("localhost:9092") {
    client {
        clientId("my-client")
    }

    producer {
        configure {
            acks(Acks.InSyncReplicas)
        }
        keySerializer(StringSerializer())
        valueSerializer(StringSerializer())

        defaultTopic("demo-topic")

        onSendError {_, _, error ->
            e.printStackTrace()
        }

        onSendSuccess{ _, _, metadata ->
            println("Record was sent successfully: topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()} ")
        }
    }
}

val messages = listOf("I ❤️ Logs", "Making Sense of Stream Processing", "Apache Kafka")
producer.use {
    producer.init() // create internal producer and call initTransaction() if `transactional.id` is set
    messages.forEach { producer.send(value = it) }
}
```

N.B: See the full source code: [ProducerKotlinDSLExample.kt](https://github.com/streamthoughts/kafka-clients-kotlin/blob/master/examples/src/main/kotlin/io/streamthoughts/kafka/client/examples/ProducerKotlinDSLExample.kt)

### Consuming messages from a Kafka topic


**Example: How to create `KafkaConsumer` config ?**

```kotlin
val configs = consumerConfigsOf()
    .client { bootstrapServers("localhost:9092") }
    .groupId("demo-consumer-group")
    .keyDeserializer(StringDeserializer::class.java.name)
    .valueDeserializer(StringDeserializer::class.java.name)
```

**Example with standard `KafkaConsumer` (i.e : using java `kafka-clients`)**

```kotlin
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
```

N.B: See the full source code: [ConsumerClientExample.kt](https://github.com/streamthoughts/kafka-clients-kotlin/blob/master/examples/src/main/kotlin/io/streamthoughts/kafka/client/examples/ConsumerClientExample.kt)

```kotlin
val consumerWorker: ConsumerWorker<String, String> = kafka("localhost:9092") {
    client {
        clientId("my-client")
    }

    val stringDeserializer: Deserializer<String> = StringDeserializer()
    consumer("my-group", stringDeserializer, stringDeserializer) {
        configure {
            maxPollRecords(1000)
            autoOffsetReset(AutoOffsetReset.Earliest)
        }

        onDeserializationError(silentlyReplaceWithNull())

        onPartitionsAssigned { _: Consumer<*, *>, partitions ->
            println("Partitions assigned: $partitions")
        }

        onPartitionsRevokedAfterCommit { _: Consumer<*, *>, partitions ->
            println("Partitions revoked: $partitions")
        }

        onConsumed { _: Consumer<*, *>, value: String? ->
            println("consumed record-value: $value")
        }

        onConsumedError(closeTaskOnConsumedError())

        Runtime.getRuntime().addShutdownHook(Thread { run { stop() } })
    }
}

consumerWorker.use {
    consumerWorker.start("demo-topic", maxParallelHint = 4)
    runBlocking {
        println("All consumers started, waiting one minute before stopping")
        delay(Duration.ofMinutes(1).toMillis())
    }
}
```

N.B: See the full source code: [ConsumerKotlinDSLExample.kt](https://github.com/streamthoughts/kafka-clients-kotlin/blob/master/examples/src/main/kotlin/io/streamthoughts/kafka/client/examples/ConsumerKotlinDSLExample.kt)

## How to build project ?

Kafka Clients for Kotlin uses [maven-wrapper](https://github.com/takari/maven-wrapper).

```bash
$ ./mvnw clean package
```

Run Tests
```bash
$ ./mvnw clean test
```

## Licence

Copyright 2020 StreamThoughts.

Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License