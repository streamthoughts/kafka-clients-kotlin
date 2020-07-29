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
package io.streamthoughts.kafka.clients.consumer.error.serialization

import io.streamthoughts.kafka.clients.loggerFor
import org.apache.kafka.clients.consumer.ConsumerRecord

object DeserializationErrorHandlers {

    fun <K, V> silentlyReplaceWith(key: K, value: V): DeserializationErrorHandler<K, V> = ReplaceErrorHandler(key, value)

    fun <K, V> silentlyReplaceWithNull(): DeserializationErrorHandler<K, V> = ReplaceErrorHandler()

    fun <K, V> logAndFail(): DeserializationErrorHandler<K, V> = LogAndSkipErrorHandler()

    fun <K, V> logAndSkip(): DeserializationErrorHandler<K, V> = LogAndFailErrorHandler()
}

private class ReplaceErrorHandler<K, V>(
    private val key: K? = null,
    private val value: V? = null
): DeserializationErrorHandler<K, V> {

    companion object  {
        private val Log = loggerFor(DeserializationErrorHandler::class.java)
    }

    override fun handle(
        record: ConsumerRecord<ByteArray, ByteArray>,
        error: Exception
    ): DeserializationErrorHandler.Response<K?, V?> {
        Log.warn("Cannot deserialize record:" +
            " topic = ${record.topic()} " +
            ", partition = ${record.partition()}" +
            ", offset ${record.topic()}" +
            ". Replace key and value.", error)
        return DeserializationErrorHandler.Response.Replace(key, value)
    }
}

private class LogAndSkipErrorHandler<K, V>: DeserializationErrorHandler<K, V> {

    companion object  {
        private val Log = loggerFor(LogAndSkipErrorHandler::class.java)
    }

    override fun handle(
        record: ConsumerRecord<ByteArray, ByteArray>,
        error: Exception
    ): DeserializationErrorHandler.Response<K?, V?> {
        Log.error("Cannot deserialize record:" +
                " topic = ${record.topic()} " +
                ", partition = ${record.partition()}" +
                ", offset ${record.topic()}" +
                ". Skip and continue.", error)
        return DeserializationErrorHandler.Response.Skip()
    }
}

private class LogAndFailErrorHandler<K, V>: DeserializationErrorHandler<K, V> {

    companion object  {
        private val Log = loggerFor(LogAndFailErrorHandler::class.java)
    }

    override fun handle(
        record: ConsumerRecord<ByteArray, ByteArray>,
        error: Exception
    ): DeserializationErrorHandler.Response<K?, V?> {
        Log.error("Cannot deserialize record:" +
                " topic = ${record.topic()} " +
                ", partition = ${record.partition()}" +
                ", offset ${record.topic()}" +
                ". Fail consumption.", error)
        return DeserializationErrorHandler.Response.Fail()
    }
}
