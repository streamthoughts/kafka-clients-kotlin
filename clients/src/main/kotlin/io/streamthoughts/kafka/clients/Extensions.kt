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

import java.io.FileInputStream
import java.io.InputStream
import java.util.Properties

/**
 * Convenient method to transform a [Properties] to a [Map] of string keys.
 */
fun Properties.toStringMap(): Map<String, Any?> = this.map { (k, v)  -> Pair(k.toString(), v) }.toMap()

/**
 * Convenient method to load config properties from the given [configFile].
 */
fun <T:Configs> T.load(configFile: String): T =
    apply { FileInputStream(configFile).use { load(it) } }

/**
 * Convenient method to load config properties from the given [inputStream].
 */
fun <T:Configs> T.load(inputStream: InputStream): T =
    apply { putAll((Properties().apply { load(inputStream) }).toStringMap()) }
