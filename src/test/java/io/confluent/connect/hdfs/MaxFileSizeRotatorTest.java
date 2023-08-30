package io.confluent.connect.hdfs;

import org.junit.Test;
import io.confluent.connect.storage.format.RecordWriter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaxFileSizeRotatorTest {

  private final long ONE_KB = 1024;
  private final long ONE_MB = ONE_KB * 1024;
  private final long MAX_CHECK_RECORDS = 30_000;

  @Test
  public void maxFileSizeOf10mb_checkMaxFileSizeRecord_returnsSizeLessThan10MB() {
    testMaxFileSizeWorks(10);
  }

  @Test
  public void maxFileSizeOf512mb_checkMaxFileSizeRecord_returnsSizeLessThan512MB() {
    testMaxFileSizeWorks(512);
  }

  private void testMaxFileSizeWorks(int maxSizeMB) {
    MaxFileSizeRotator maxFileSizeRotator = new MaxFileSizeRotator(ONE_MB * maxSizeMB);
    SizeAwareRecordWriter sizeAwareRecordWriter = mock(SizeAwareRecordWriter.class);
    assertTrue(findRolloverSizeIncrementing100kbRecords(sizeAwareRecordWriter, maxFileSizeRotator) < ONE_MB * maxSizeMB);
  }

  @Test
  public void aNonSizeAwareRecordWriter_checkMaxFileSizeReached_returnsFalse() {
    MaxFileSizeRotator maxFileSizeRotator = new MaxFileSizeRotator(ONE_MB);
    RecordWriter nonSizeAwareRecordWriter = mock(RecordWriter.class);
    assertFalse( maxFileSizeRotator.checkMaxFileSizeReached(nonSizeAwareRecordWriter, 100000));
  }

  private long findRolloverSizeIncrementing100kbRecords(SizeAwareRecordWriter recordWriter, MaxFileSizeRotator maxFileSizeRotator) {
    long fileSize = 0;
    for(int recordCount = 1; recordCount < MAX_CHECK_RECORDS; recordCount++) {
      fileSize += ONE_KB * 100;
      when(recordWriter.getDataSize()).thenReturn(fileSize);
      if (maxFileSizeRotator.checkMaxFileSizeReached(recordWriter, recordCount)) {
        return fileSize;
      }
    }

    return -1;
  }
}
