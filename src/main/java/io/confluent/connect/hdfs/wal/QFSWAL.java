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
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QFSWAL implements WAL {
  private static final String LOCK_FILE_EXTENSION = "lock-qfs";

  private static final Duration DEFAULT_LOCK_REFRESH_INTERVAL = Duration.ofSeconds(10);
  private static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(60);
  private final UUID uuid;
  private final Timer timer;
  private final HdfsStorage storage;
  private final String logsDir;
  private final TopicPartition topicPartition;
  private final Pattern pattern;
  private final String lockDir;
  private LockFile lockFile;
  private final Duration lockRefreshInterval;
  private final Duration lockTimeout;
  private static final Logger log = LoggerFactory.getLogger(QFSWAL.class);

  public QFSWAL(String logsDir, TopicPartition topicPartition, HdfsStorage storage) {
    this(logsDir,
        topicPartition,
        storage,
        DEFAULT_LOCK_REFRESH_INTERVAL,
        DEFAULT_LOCK_TIMEOUT
    );
  }

  public QFSWAL(
          String logsDir,
          TopicPartition topicPartition,
          HdfsStorage storage,
          Duration lockRefreshInterval,
          Duration lockTimeout) {
    this.uuid = UUID.randomUUID();
    this.pattern = Pattern.compile(String.format(
            "(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
                    + "-(?<epoch>\\d+)(\\.)%s",
            LOCK_FILE_EXTENSION));
    this.timer = new Timer("QFSWAL-Timer", true);
    this.storage = storage;
    this.logsDir = logsDir;
    this.topicPartition = topicPartition;
    this.lockDir = FileUtils.directoryName(storage.url(), logsDir, topicPartition);
    this.lockRefreshInterval = lockRefreshInterval;
    this.lockTimeout = lockTimeout;
    this.lockFile = this.createOrRenameLockFile();
    this.startRenamingTimer();
  }

  static class LockFile {
    public final String storageURL;
    public final String logsDir;
    public final TopicPartition topicPartition;
    public final UUID uuid;
    public final Instant instant;

    LockFile(
            String storageURL,
            String logsDir,
            TopicPartition topicPartition,
            UUID uuid,
            Instant instant
    ) {
      this.storageURL = storageURL;
      this.logsDir = logsDir;
      this.topicPartition = topicPartition;
      this.uuid = uuid;
      this.instant = instant;
    }

    public String filePath() {
      return FileUtils.fileName(
              storageURL,
              logsDir,
              topicPartition,
              String.format("%s-%s.%s", uuid, instant.toEpochMilli(), LOCK_FILE_EXTENSION)
      );
    }
  }

  private LockFile getNewLockFile() {
    return new LockFile(
            this.storage.url(),
            this.logsDir,
            this.topicPartition,
            this.uuid,
            Instant.now()
    );
  }

  private LockFile createOrRenameLockFile() {
    if (!this.storage.exists(this.lockDir)) {
      this.storage.create(this.lockDir);
    }

    LockFile newLockFile = getNewLockFile();

    List<LockFile> liveLockFiles = this.findLockFiles()
            .stream()
            .filter(l -> l.instant.isAfter(Instant.now().minus(this.lockTimeout)))
            .collect(Collectors.toList());

    if (!liveLockFiles.isEmpty()) {
      throw new ConnectException("Lock has been acquired by another process");
    }

    this.storage.create(newLockFile.filePath(), true);

    liveLockFiles = this.findLockFiles()
            .stream()
            .filter(l -> l.instant.isAfter(Instant.now().minus(this.lockTimeout)))
            .collect(Collectors.toList());

    if (liveLockFiles.size() > 1) {
      this.storage.delete(newLockFile.filePath());
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
    }, this.lockRefreshInterval.toMillis(), this.lockRefreshInterval.toMillis());
  }

  private void renameLockFile() {
    LockFile newLockFile = getNewLockFile();

    try {
      this.storage.commit(this.lockFile.filePath(), newLockFile.filePath());
      this.lockFile = newLockFile;
    } catch (Exception e) {
      log.error("Failed to rename the file", e);
    }
  }

  private List<LockFile> findLockFiles() {
    return this.storage.list(this.lockDir)
            .stream()
            .filter(FileStatus::isFile)
            .map(FileStatus::getPath)
            .map(Path::getName)
            .map(pattern::matcher)
            .filter(Matcher::matches)
            .map(m ->
              new LockFile(
                this.storage.url(),
                this.logsDir,
                this.topicPartition,
                UUID.fromString(m.group("uuid")),
                Instant.ofEpochSecond(Long.parseLong(m.group("epoch")))
            )).collect(Collectors.toList());
  }

  @Override
  public void acquireLease() throws ConnectException {
    List<LockFile> lockFiles = this.findLockFiles();

    if (lockFiles.isEmpty()) {
      throw new ConnectException("The lock file is not present in the log dir");
    }

    if (lockFiles.size() > 1) {
      throw new ConnectException("More than one lock file reside in the log dir");
    }

    LockFile lockFile = lockFiles.get(0);

    if (!lockFile.uuid.equals(this.uuid)) {
      log.error(
              "UUID of the lock file {} for {}-{} topic-partition does not match {} uuid",
              lockFile.filePath(),
              this.topicPartition.topic(),
              this.topicPartition.partition(),
              this.uuid);
      throw new ConnectException("Lock uuid does not match");
    }

    if (lockFile.instant.isBefore(Instant.now().minus(this.lockTimeout))) {
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
    this.storage.delete(this.lockFile.filePath());
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
