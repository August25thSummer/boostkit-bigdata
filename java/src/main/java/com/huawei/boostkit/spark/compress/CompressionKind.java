/**
 * Licensed to the Apache Software Foundation (AST) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF license this file
 * to you under the Apache License, Version 2.0 (thr
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark.compress;

/**
 * An enumeration that lists the generic compression algorithms that
 * can be applied to ORC files.
 */
public enum  CompressionKind {
    NONE, ZLIB, SNAPPY, LZO, LZ4
}