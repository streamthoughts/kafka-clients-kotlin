# Kafka Clients for Kotlin

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/streamthoughts/kafka-connect-file-pulse/blob/master/LICENSE)

## Warning

Be aware that this package is still in heavy development. Some breaking change will occur in future weeks and months.
Thank's for your comprehension.

## What is Kafka Clients for Kotlin ?

The **Kafka Client for Kotlin** projects packs with convenient Kotlin API for the development of Kafka-based event-driven applications.
It provides high-level abstractions both for sending records (`ProducerContainer`) and consuming records from topics using one or many
concurrent consumers (`KafkaConsumerWorker`).

**Kafka Client for Kotlin** is based on the pure java `kafka-clients`.

## How to contribute ?

The project is in its early stages so it can be very easy to contribute by proposing APIs changes, new features and so one. 
Any feedback, bug reports and PRs are greatly appreciated!

* Source Code: https://github.com/streamthoughts/kafka-clients-kotlin
* Issue Tracker: https://github.com/streamthoughts/kafka-clients-kotlin/issues

## Show your support

You think this project can help you or your team to develop kafka-based application with Kotlin ?

Please ⭐ this repository to support us!

## How to give it a try ?

Just add **Kafka Client for Kotlin** to the dependencies of your projects. 

### For Maven
```xml
<dependency>
  <groupId>io.streamthoughts</groupId>
  <artifactId>kafka-clients-kotlin</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Getting Started

### Kafka Producer

See the full code-snippet : [ProducerExample.kt](https://github.com/streamthoughts/kafka-clients-kotlin/blob/master/examples/src/main/kotlin/io/streamthoughts/kafka/client/examples/ProducerExample.kt)

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

        onSendSuccess{ _, _, metadata ->
            println("Record was sent successfully: topic=${metadata.topic()}, partition=${metadata.partition()}, offset=${metadata.offset()} ")
        }
    }
}

with(producer) {
    init()
    listOf("I ❤️ Logs", "Making Sense of Stream Processing", "Apache Kafka").forEach {
        send(value = it)
    }
    close()
}
```

### Kafka Consumer

See the full code-snippet : [ConsumerExample.kt](https://github.com/streamthoughts/kafka-clients-kotlin/blob/master/examples/src/main/kotlin/io/streamthoughts/kafka/client/examples/ConsumerExample.kt)

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

        onDeserializationError(DeserializationErrorHandlers.silentlyReplaceWithNull())

        onPartitionsAssigned { _: Consumer<*, *>, partitions ->
            println("Partitions assigned: $partitions")
        }

        onPartitionsRevokedAfterCommit { _: Consumer<*, *>, partitions ->
            println("Partitions revoked: $partitions")
        }

        onConsumed { _: Consumer<*, *>, value: String? ->
            println("consumed record-value: $value")
        }

        Runtime.getRuntime().addShutdownHook(Thread { run { stop() } })
    }
}
consumerWorker.start("topic-test", maxParallelHint = 4)
```

## Licence

Copyright 2020 StreamThoughts.

Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License