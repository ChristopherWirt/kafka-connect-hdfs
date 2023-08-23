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

import io.confluent.connect.storage.wal.FilePathOffset;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * NoopWAL - is a blank implementation of WAL - it can be used for testing the WAL fallback
 * methods
 */
public class NoopWAL implements WAL {
  @Override
  public void acquireLease() throws ConnectException {

  }

  @Override
  public void append(String s, String s1) throws ConnectException {

  }

  @Override
  public void apply() throws ConnectException {

  }

  @Override
  public void truncate() throws ConnectException {

  }

  @Override
  public void close() throws ConnectException {

  }

  @Override
  public String getLogFile() {
    return null;
  }

  @Override
  public FilePathOffset extractLatestOffset() {
    return null;
  }
}
