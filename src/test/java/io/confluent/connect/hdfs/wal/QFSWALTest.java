package io.confluent.connect.hdfs.wal;

import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class QFSWALTest extends TestWithMiniDFSCluster {

    @Test
    public void testLockCreating() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        new QFSWAL("/logs", tp, storage);
        List<FileStatus> fs = storage.list("/logs/mytopic/123/");
        assertEquals(1, fs.size());
    }

    @Test(expected = ConnectException.class)
    public void testSecondProcessCannotAcquireLock() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        new QFSWAL("/logs", tp, storage);
        new QFSWAL("/logs", tp, storage);
        List<FileStatus> fs = storage.list("/logs/mytopic/123/");
        assertEquals(1, fs.size());
    }

    @Test
    public void testLockGetsRenamed() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        QFSWAL wal = new QFSWAL("/logs", tp, storage, 1000, 2000);
        String fileName1 = storage.list("/logs/mytopic/123/").get(0).getPath().getName();

        Thread.sleep(2000);
        String fileName2 = storage.list("/logs/mytopic/123/").get(0).getPath().getName();

        Thread.sleep(2000);
        String fileName3 = storage.list("/logs/mytopic/123/").get(0).getPath().getName();

        assertNotEquals(fileName1, fileName2);
        assertNotEquals(fileName2, fileName3);
    }

    @Test(expected = ConnectException.class)
    public void testLockGetsTimedOut() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        QFSWAL wal = new QFSWAL("/logs", tp, storage, 1000, 2000);
        wal.close();

        Thread.sleep(3000);
        wal.acquireLease();
    }

    @Test
    public void testAcquireLease() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        QFSWAL wal = new QFSWAL("/logs", tp, storage, 1000, 2000);

        Thread.sleep(1500);
        wal.acquireLease();
    }

    @Test
    public void testAnotherProcessCanGrabLockAfterTimeout() throws Exception {
        setUp();
        HdfsStorage storage = new HdfsStorage(connectorConfig, url);
        TopicPartition tp = new TopicPartition("mytopic", 123);
        (new QFSWAL("/logs", tp, storage, 1000, 2000)).close();

        Thread.sleep(3000);
        QFSWAL wal = new QFSWAL("/logs", tp, storage, 1000, 2000);
        wal.acquireLease();
    }
}
