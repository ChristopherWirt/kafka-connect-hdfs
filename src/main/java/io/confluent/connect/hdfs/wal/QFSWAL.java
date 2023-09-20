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

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.wal.FilePathOffset;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QFSWAL implements WAL {
  private static final String LOCK_FILE_EXTENSION = "lock-qfs";
  private static final int DEFAULT_LOCK_REFRESH_INTERVAL_IN_MS = 10_000;
  private static final int DEFAULT_LOCK_TIMEOUT_IN_MS = 60_000;
  private final UUID uuid;
  private final Timer timer;
  private final HdfsStorage storage;
  private final String logsDir;
  private final TopicPartition topicPartition;
  private final Pattern pattern;
  private final String lockDir;
  private String lockFile;

  private final int lockRefreshIntervalInMs;
  private final int lockTimeoutInMs;
  private static final Logger log = LoggerFactory.getLogger(QFSWAL.class);

  public QFSWAL(String logsDir, TopicPartition topicPartition, HdfsStorage storage) {
    this(logsDir,
         topicPartition,
         storage,
         DEFAULT_LOCK_REFRESH_INTERVAL_IN_MS,
         DEFAULT_LOCK_TIMEOUT_IN_MS);
  }

  public QFSWAL(
          String logsDir,
          TopicPartition topicPartition,
          HdfsStorage storage,
          int lockRefreshIntervalInMs,
          int lockTimeoutInMs
  ) {
    this.uuid = UUID.randomUUID();
    this.pattern = Pattern.compile(String.format(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-(\\d+)(\\.)%s",
            LOCK_FILE_EXTENSION));
    this.timer = new Timer("QFSWAL-Timer", true);
    this.storage = storage;
    this.logsDir = logsDir;
    this.topicPartition = topicPartition;
    this.lockDir = FileUtils.directoryName(storage.url(), logsDir, topicPartition);
    this.lockRefreshIntervalInMs = lockRefreshIntervalInMs;
    this.lockTimeoutInMs = lockTimeoutInMs;
    this.lockFile = this.createOrRenameLockFile();
    this.startRenamingTimer();
  }

  private String getNewLockFile() {
    return FileUtils.fileName(storage.url(),
                              this.logsDir,
                              this.topicPartition,
                              String.format("%s-%s.%s",
                                            this.uuid,
                                            System.currentTimeMillis(),
                                            LOCK_FILE_EXTENSION));
  }

  private String createOrRenameLockFile() {
    if (!this.storage.exists(this.lockDir)) {
      this.storage.create(this.lockDir);
    }

    List<SimpleEntry<String, Matcher>> lockFiles = this.findLockFiles();
    String newLockFile = getNewLockFile();

    long liveLocksCount = lockFiles.stream()
            .filter(pair -> Long.parseLong(
                    pair.getValue().group(2))
                    > System.currentTimeMillis() - this.lockTimeoutInMs
             ).count();

    if (liveLocksCount != 0) {
      throw new ConnectException("Lock has been acquired by another process");
    }

    if (lockFiles.isEmpty()) {
      this.storage.create(newLockFile, true);
    } else {
      String oldLockFile = FileUtils.fileName(storage.url(),
                                              this.logsDir,
                                              this.topicPartition,
                                              lockFiles.get(0).getKey());
      this.storage.commit(oldLockFile, newLockFile);
    }

    liveLocksCount = this.findLockFiles()
            .stream()
            .filter(pair -> Long.parseLong(
                    pair.getValue().group(2))
                    > System.currentTimeMillis() - this.lockTimeoutInMs
            ).count();

    if (liveLocksCount > 1) {
      this.storage.delete(newLockFile);
      throw new ConnectException("Lock has been acquired by another process");
    }

    return newLockFile;
  }


  private void startRenamingTimer() {
    this.timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        renameLockFile();
      }
    }, this.lockRefreshIntervalInMs, this.lockTimeoutInMs);
  }

  private void renameLockFile() {
    String newLockFile = getNewLockFile();

    try {
      this.storage.commit(this.lockFile, newLockFile);
      this.lockFile = newLockFile;
    } catch (Exception e) {
      log.error("Failed to rename the file", e);
    }
  }

  private List<SimpleEntry<String, Matcher>> findLockFiles() {
    List<FileStatus> walFiles = this.storage.list(this.lockDir);

    return walFiles.stream()
            .filter(fileStatus -> fileStatus.isFile())
            .map((Function<FileStatus, Object>) fileStatus -> fileStatus.getPath()
                    .getName())
            .map(Object::toString)
            .map(fileName -> new SimpleEntry<String, Matcher>(fileName,
                                                              pattern.matcher(fileName)))
            .filter(pair -> pair.getValue().matches())
            .collect(Collectors.toList());
  }

  @Override
  public void acquireLease() throws ConnectException {
    List<SimpleEntry<String, Matcher>> lockFiles = this.findLockFiles();
    if (lockFiles.size() == 0) {
      throw new ConnectException("The lock file is not present in the log dir");
    }

    if (lockFiles.size() > 1) {
      throw new ConnectException("More than one lock file reside in the log dir");
    }

    String filename = lockFiles.get(0).getKey();
    Matcher matcher = lockFiles.get(0).getValue();

    if (!matcher.group(1).equals(this.uuid.toString())) {
      log.error(
              "UUID of the lock file %s for %s-%s topic-partition does not match %s uuid",
              filename,
              this.topicPartition.topic(),
              this.topicPartition.partition(),
              this.uuid);
      throw new ConnectException("Lock uuid does not match");
    }

    if (Long.parseLong(matcher.group(2)) < System.currentTimeMillis() - this.lockTimeoutInMs) {
      throw new ConnectException(
              "Lock file has not been renamed for more than the threshold");
    }
  }

  @Override
  public void append(String s, String s1) throws ConnectException {
    this.acquireLease();
  }

  @Override
  public void apply() throws ConnectException {
  }

  @Override
  public void truncate() throws ConnectException {
  }

  @Override
  public void close() throws ConnectException {
    this.timer.cancel();
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
