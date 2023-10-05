/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.wal;


import io.confluent.connect.hdfs.storage.HdfsStorage;
import org.apache.kafka.common.TopicPartition;

public enum WalType {
  NOOP((storage, logsDir, tp) -> new NoopWAL()),

  HDFS((logsDir, tp, storage) -> storage.hdfsWAL(logsDir, tp)),
  QFS((logsDir, tp, storage) -> storage.qfsWAL(logsDir, tp));

  interface BuilderFunction<A, B, C, D> {
    D apply(A a, B b, C c);
  }

  private final BuilderFunction<String, TopicPartition, HdfsStorage, WAL> factoryMethod;

  WalType(BuilderFunction<String, TopicPartition, HdfsStorage, WAL> factoryMethod) {
    this.factoryMethod = factoryMethod;
  }

  public WAL create(String logsDir, TopicPartition tp, HdfsStorage storage) {
    return factoryMethod.apply(logsDir, tp, storage);
  }
}
