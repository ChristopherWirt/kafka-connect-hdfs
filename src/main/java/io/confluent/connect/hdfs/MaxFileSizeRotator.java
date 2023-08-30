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

package io.confluent.connect.hdfs;

import io.confluent.connect.storage.format.RecordWriter;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;

/**
 * The point of this class is to reduce the number of calls to writer.getDataSize() because for many
 * RecordWriter implementations calculating the total fileSize is an expensive operation.
 */
public class MaxFileSizeRotator {
  private final long maxFileSizeBytes;

  private long recordCountForNextMemCheck = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;

  public MaxFileSizeRotator(long maxFileSize) {
    this.maxFileSizeBytes = maxFileSize;
  }

  public boolean checkMaxFileSizeReached(RecordWriter recordWriter, long recordCount) {
    SizeAwareRecordWriter writer = (recordWriter instanceof SizeAwareRecordWriter)
        ? (SizeAwareRecordWriter) recordWriter
        : null;
    if (writer == null || maxFileSizeBytes == -1) {
      return false;
    }

    if (recordCount > recordCountForNextMemCheck) {

      // Modified for readability version of InternalParquetRecordWriter.checkBlockSizeReached()
      long memSize = writer.getDataSize();
      long recordSize = memSize / recordCount;

      // count it as maxFileSize if within ~2 records of the limit
      // it is much better to be slightly undersized than to be over at all
      if (memSize > (maxFileSizeBytes - 2 * recordSize)) {
        recordCountForNextMemCheck = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
        return true;
      }

      long maxRecordGuess = (long) (maxFileSizeBytes / ((float) recordSize));
      long halfWayGuess = (recordCount + maxRecordGuess) / 2;
      recordCountForNextMemCheck = min(
          max(DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK, halfWayGuess),
          // will not look more than max records ahead
          recordCount
              + DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK
      );
    }
    return false;
  }
}
