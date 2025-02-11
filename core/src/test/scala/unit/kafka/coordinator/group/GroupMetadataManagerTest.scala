/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.coordinator.group

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collections, Optional}
import com.yammer.metrics.core.Gauge

import javax.management.ObjectName
import kafka.api._
import kafka.cluster.Partition
import kafka.common.OffsetAndMetadata
import kafka.log.{AppendOrigin, Log, LogAppendInfo}
import kafka.metrics.KafkaYammerMetrics
import kafka.server.{FetchDataInfo, FetchLogEnd, HostedPartition, KafkaConfig, LogOffsetMetadata, ReplicaManager, RequestLocal}
import kafka.utils.{KafkaScheduler, MockTime, TestUtils}
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetricsContext, Metrics => kMetrics}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Utils
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection._

class GroupMetadataManagerTest {

  var time: MockTime = null
  var replicaManager: ReplicaManager = null
  var groupMetadataManager: GroupMetadataManager = null
  var scheduler: KafkaScheduler = null
  var partition: Partition = null
  var defaultOffsetRetentionMs = Long.MaxValue
  var metrics: kMetrics = null

  val groupId = "foo"
  val groupInstanceId = "bar"
  val groupPartitionId = 0
  val groupTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId)
  val protocolType = "protocolType"
  val rebalanceTimeout = 60000
  val sessionTimeout = 10000
  val defaultRequireStable = false
  val numOffsetsPartitions = 2

  private val offsetConfig = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(nodeId = 0, zkConnect = ""))
    OffsetConfig(maxMetadataSize = config.offsetMetadataMaxSize,
      loadBufferSize = config.offsetsLoadBufferSize,
      offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
      offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
      offsetsTopicNumPartitions = config.offsetsTopicPartitions,
      offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
      offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
      offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
      offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
      offsetCommitRequiredAcks = config.offsetCommitRequiredAcks)
  }

  @BeforeEach
  def setUp(): Unit = {
    defaultOffsetRetentionMs = offsetConfig.offsetsRetentionMs
    metrics = new kMetrics()
    time = new MockTime
    replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
    groupMetadataManager = new GroupMetadataManager(0, ApiVersion.latestVersion, offsetConfig, replicaManager,
      time, metrics)
    groupMetadataManager.startup(() => numOffsetsPartitions, false)
    partition = EasyMock.niceMock(classOf[Partition])
  }

  @AfterEach
  def tearDown(): Unit = {
    groupMetadataManager.shutdown()
  }

  @Test
  def testLogInfoFromCleanupGroupMetadata(): Unit = {
    var expiredOffsets: Int = 0
    var infoCount = 0
    val gmm = new GroupMetadataManager(0, ApiVersion.latestVersion, offsetConfig, replicaManager, time, metrics) {
      override def cleanupGroupMetadata(groups: Iterable[GroupMetadata], requestLocal: RequestLocal,
                                        selector: GroupMetadata => Map[TopicPartition, OffsetAndMetadata]): Int = expiredOffsets

      override def info(msg: => String): Unit = infoCount += 1
    }
    gmm.startup(() => numOffsetsPartitions, false)
    try {
      // if there are no offsets to expire, we skip to log
      gmm.cleanupGroupMetadata()
      assertEquals(0, infoCount)
      // if there are offsets to expire, we should log info
      expiredOffsets = 100
      gmm.cleanupGroupMetadata()
      assertEquals(1, infoCount)
    } finally {
      gmm.shutdown()
    }
  }

  @Test
  def testLoadOffsetsWithoutGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, offsetCommitRecords.toArray: _*)
    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadEmptyGroupWithOffsets(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 15
    val protocolType = "consumer"
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildEmptyGroupRecord(generation, protocolType)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.leaderOrNull)
    assertNull(group.protocolName.orNull)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadTransactionalOffsetsWithoutGroup(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testDoNotLoadAbortedTransactionalOffsetCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val abortedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = false)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // Since there are no committed offsets for the group, and there is no other group metadata, we don't expect the
    // group to be loaded.
    assertEquals(None, groupMetadataManager.getGroup(groupId))
  }

  @Test
  def testGroupLoadedWithPendingCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    val bar0 = new TopicPartition("bar", 0)
    val pendingOffsets = Map(
      foo0 -> 23L,
      foo1 -> 455L,
      bar0 -> 8992L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    // Ensure that no offsets are materialized, but that we have offsets pending.
    assertEquals(0, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(foo0))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(foo1))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(bar0))
  }

  @Test
  def testLoadWithCommittedAndAbortedTransactionalOffsetCommits(): Unit = {
    // A test which loads a log with a mix of committed and aborted transactional offset committed messages.
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val abortedOffsets = Map(
      new TopicPartition("foo", 2) -> 231L,
      new TopicPartition("foo", 3) -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = false)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
  }

  @Test
  def testLoadWithCommittedAndAbortedAndPendingTransactionalOffsetCommits(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val foo3 = new TopicPartition("foo", 3)

    val abortedOffsets = Map(
      new TopicPartition("foo", 2) -> 231L,
      foo3 -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val pendingOffsets = Map(
      foo3 -> 2312L,
      new TopicPartition("foo", 4) -> 45512L,
      new TopicPartition("bar", 2) -> 899212L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    val commitOffsetsLogPosition = nextOffset
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = false)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)

    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(commitOffsetsLogPosition), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }

    // We should have pending commits.
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(foo3))

    // The loaded pending commits should materialize after a commit marker comes in.
    groupMetadataManager.handleTxnCompletion(producerId, List(groupMetadataTopicPartition.partition).toSet, isCommit = true)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    pendingOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadTransactionalOffsetCommitsFromMultipleProducers(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val firstProducerId = 1000L
    val firstProducerEpoch: Short = 2
    val secondProducerId = 1001L
    val secondProducerEpoch: Short = 3
    val groupEpoch = 2

    val committedOffsetsFirstProducer = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val committedOffsetsSecondProducer = Map(
      new TopicPartition("foo", 2) -> 231L,
      new TopicPartition("foo", 3) -> 4551L,
      new TopicPartition("bar", 1) -> 89921L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0L

    val firstProduceRecordOffset = nextOffset
    nextOffset += appendTransactionalOffsetCommits(buffer, firstProducerId, firstProducerEpoch, nextOffset, committedOffsetsFirstProducer)
    nextOffset += completeTransactionalOffsetCommit(buffer, firstProducerId, firstProducerEpoch, nextOffset, isCommit = true)

    val secondProducerRecordOffset = nextOffset
    nextOffset += appendTransactionalOffsetCommits(buffer, secondProducerId, secondProducerEpoch, nextOffset, committedOffsetsSecondProducer)
    nextOffset += completeTransactionalOffsetCommit(buffer, secondProducerId, secondProducerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)

    // Ensure that only the committed offsets are materialized, and that there are no pending commits for the producer.
    // This allows us to be certain that the aborted offset commits are truly discarded.
    assertEquals(committedOffsetsFirstProducer.size + committedOffsetsSecondProducer.size, group.allOffsets.size)
    committedOffsetsFirstProducer.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(firstProduceRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
    committedOffsetsSecondProducer.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(secondProducerRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
  }

  @Test
  def testGroupLoadWithConsumerAndTransactionalOffsetCommitsConsumerWins(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val transactionalOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 23L
    )

    val consumerOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 24L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, transactionalOffsetCommits)
    val consumerRecordOffset = nextOffset
    nextOffset += appendConsumerOffsetCommit(buffer, nextOffset, consumerOffsetCommits)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(1, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(consumerOffsetCommits.size, group.allOffsets.size)
    consumerOffsetCommits.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertEquals(Some(consumerRecordOffset), group.offsetWithRecordMetadata(topicPartition).head.appendedBatchOffset)
    }
  }

  @Test
  def testGroupLoadWithConsumerAndTransactionalOffsetCommitsTransactionWins(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val producerId = 1000L
    val producerEpoch: Short = 2
    val groupEpoch = 2

    val transactionalOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 23L
    )

    val consumerOffsetCommits = Map(
      new TopicPartition("foo", 0) -> 24L
    )

    val buffer = ByteBuffer.allocate(1024)
    var nextOffset = 0
    nextOffset += appendConsumerOffsetCommit(buffer, nextOffset, consumerOffsetCommits)
    nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, transactionalOffsetCommits)
    nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, isCommit = true)
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    expectGroupMetadataLoad(groupMetadataTopicPartition, 0, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // The group should be loaded with pending offsets.
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(1, group.allOffsets.size)
    assertTrue(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(consumerOffsetCommits.size, group.allOffsets.size)
    transactionalOffsetCommits.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testGroupNotExists(): Unit = {
    // group is not owned
    assertFalse(groupMetadataManager.groupNotExists(groupId))

    groupMetadataManager.addPartitionOwnership(groupPartitionId)
    // group is owned but does not exist yet
    assertTrue(groupMetadataManager.groupNotExists(groupId))

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // group is owned but not Dead
    assertFalse(groupMetadataManager.groupNotExists(groupId))

    group.transitionTo(Dead)
    // group is owned and Dead
    assertTrue(groupMetadataManager.groupNotExists(groupId))
  }

  private def appendConsumerOffsetCommit(buffer: ByteBuffer, baseOffset: Long, offsets: Map[TopicPartition, Long]) = {
    val builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.LOG_APPEND_TIME, baseOffset)
    val commitRecords = createCommittedOffsetRecords(offsets)
    commitRecords.foreach(builder.append)
    builder.build()
    offsets.size
  }

  private def appendTransactionalOffsetCommits(buffer: ByteBuffer, producerId: Long, producerEpoch: Short,
                                               baseOffset: Long, offsets: Map[TopicPartition, Long]): Int = {
    val builder = MemoryRecords.builder(buffer, CompressionType.NONE, baseOffset, producerId, producerEpoch, 0, true)
    val commitRecords = createCommittedOffsetRecords(offsets)
    commitRecords.foreach(builder.append)
    builder.build()
    offsets.size
  }

  private def completeTransactionalOffsetCommit(buffer: ByteBuffer, producerId: Long, producerEpoch: Short, baseOffset: Long,
                                                isCommit: Boolean): Int = {
    val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
      TimestampType.LOG_APPEND_TIME, baseOffset, time.milliseconds(), producerId, producerEpoch, 0, true, true,
      RecordBatch.NO_PARTITION_LEADER_EPOCH)
    val controlRecordType = if (isCommit) ControlRecordType.COMMIT else ControlRecordType.ABORT
    builder.appendEndTxnMarker(time.milliseconds(), new EndTransactionMarker(controlRecordType, 0))
    builder.build()
    1
  }

  @Test
  def testLoadOffsetsWithTombstones(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2

    val tombstonePartition = new TopicPartition("foo", 1)
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      tombstonePartition -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val tombstone = new SimpleRecord(GroupMetadataManager.offsetCommitKey(groupId, tombstonePartition), null)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(tombstone)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size - 1, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      if (topicPartition == tombstonePartition)
        assertEquals(None, group.offset(topicPartition))
      else
        assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadOffsetsAndGroup(): Unit = {
    loadOffsetsAndGroup(groupTopicPartition, 2)
  }

  def loadOffsetsAndGroup(groupMetadataTopicPartition: TopicPartition, groupEpoch: Int): GroupMetadata = {
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestamp).contains(None))
    }
    group
  }

  @Test
  def testLoadOffsetsAndGroupIgnored(): Unit = {
    val groupEpoch = 2
    loadOffsetsAndGroup(groupTopicPartition, groupEpoch)
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, Some(groupEpoch), _ => ())
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
      "Removed group remained in cache")
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, groupEpoch - 1, _ => (), 0L)
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
      "Removed group remained in cache")
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))
  }

  @Test
  def testUnloadOffsetsAndGroup(): Unit = {
    val groupEpoch = 2
    loadOffsetsAndGroup(groupTopicPartition, groupEpoch)

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, Some(groupEpoch), _ => ())
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
    "Removed group remained in cache")
  }

  @Test
  def testUnloadOffsetsAndGroupIgnored(): Unit = {
    val groupEpoch = 2
    val initiallyLoaded = loadOffsetsAndGroup(groupTopicPartition, groupEpoch)

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, Some(groupEpoch - 1), _ => ())
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()))
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(initiallyLoaded.groupId, group.groupId)
    assertEquals(initiallyLoaded.currentState, group.currentState)
    assertEquals(initiallyLoaded.leaderOrNull, group.leaderOrNull)
    assertEquals(initiallyLoaded.generationId, group.generationId)
    assertEquals(initiallyLoaded.protocolType, group.protocolType)
    assertEquals(initiallyLoaded.protocolName.orNull, group.protocolName.orNull)
    assertEquals(initiallyLoaded.allMembers, group.allMembers)
    assertEquals(initiallyLoaded.allOffsets.size, group.allOffsets.size)
    initiallyLoaded.allOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition))
      assertTrue(group.offset(topicPartition).map(_.expireTimestamp).contains(None))
    }
  }

  @Test
  def testUnloadOffsetsAndGroupIgnoredAfterStopReplica(): Unit = {
    val groupEpoch = 2
    val initiallyLoaded = loadOffsetsAndGroup(groupTopicPartition, groupEpoch)

    groupMetadataManager.removeGroupsAndOffsets(groupTopicPartition, None, _ => ())
    assertTrue(groupMetadataManager.getGroup(groupId).isEmpty,
      "Removed group remained in cache")
    assertEquals(groupEpoch, groupMetadataManager.epochForPartitionId.get(groupTopicPartition.partition()),
    "Replica which was stopped still in epochForPartitionId")

    EasyMock.reset(replicaManager)
    loadOffsetsAndGroup(groupTopicPartition, groupEpoch + 1)
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(initiallyLoaded.groupId, group.groupId)
    assertEquals(initiallyLoaded.currentState, group.currentState)
    assertEquals(initiallyLoaded.leaderOrNull, group.leaderOrNull)
    assertEquals(initiallyLoaded.generationId, group.generationId)
    assertEquals(initiallyLoaded.protocolType, group.protocolType)
    assertEquals(initiallyLoaded.protocolName.orNull, group.protocolName.orNull)
    assertEquals(initiallyLoaded.allMembers, group.allMembers)
    assertEquals(initiallyLoaded.allOffsets.size, group.allOffsets.size)
    initiallyLoaded.allOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition))
      assertTrue(group.offset(topicPartition).map(_.expireTimestamp).contains(None))
    }
  }

  @Test
  def testLoadGroupWithTombstone(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId)
    val groupMetadataTombstone = new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      Seq(groupMetadataRecord, groupMetadataTombstone).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    assertEquals(None, groupMetadataManager.getGroup(groupId))
  }

  @Test
  def testLoadGroupWithLargeGroupMetadataRecord(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    // create a GroupMetadata record larger then offsets.load.buffer.size (here at least 16 bytes larger)
    val assignmentSize = OffsetConfig.DefaultLoadBufferSize + 16
    val memberId = "98098230493"

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId, new Array[Byte](assignmentSize))
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadGroupAndOffsetsWithCorruptedLog(): Unit = {
    // Simulate a case where startOffset < endOffset but log is empty. This could theoretically happen
    // when all the records are expired and the active segment is truncated or when the partition
    // is accidentally corrupted.
    val startOffset = 0L
    val endOffset = 10L
    val groupEpoch = 2

    val logMock: Log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLog(groupTopicPartition)).andStubReturn(Some(logMock))
    expectGroupMetadataLoad(logMock, startOffset, MemoryRecords.EMPTY)
    EasyMock.expect(replicaManager.getLogEndOffset(groupTopicPartition)).andStubReturn(Some(endOffset))
    EasyMock.replay(logMock)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, groupEpoch, _ => (), 0L)

    EasyMock.verify(logMock)
    EasyMock.verify(replicaManager)

    assertFalse(groupMetadataManager.isPartitionLoading(groupTopicPartition.partition()))
  }

  @Test
  def testOffsetWriteAfterGroupRemoved(): Unit = {
    // this test case checks the following scenario:
    // 1. the group exists at some point in time, but is later removed (because all members left)
    // 2. a "simple" consumer (i.e. not a consumer group) then uses the same groupId to commit some offsets

    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 293
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )
    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val groupMetadataTombstone = new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (Seq(groupMetadataRecord, groupMetadataTombstone) ++ offsetCommitRecords).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testLoadGroupAndOffsetsFromDifferentSegments(): Unit = {
    val generation = 293
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("bar", 0)
    val tp3 = new TopicPartition("xxx", 0)

    val logMock: Log = EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLog(groupTopicPartition)).andStubReturn(Some(logMock))

    val segment1MemberId = "a"
    val segment1Offsets = Map(tp0 -> 23L, tp1 -> 455L, tp3 -> 42L)
    val segment1Records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (createCommittedOffsetRecords(segment1Offsets) ++ Seq(buildStableGroupRecordWithMember(
        generation, protocolType, protocol, segment1MemberId))).toArray: _*)
    val segment1End = expectGroupMetadataLoad(logMock, startOffset, segment1Records)

    val segment2MemberId = "b"
    val segment2Offsets = Map(tp0 -> 33L, tp2 -> 8992L, tp3 -> 10L)
    val segment2Records = MemoryRecords.withRecords(segment1End, CompressionType.NONE,
      (createCommittedOffsetRecords(segment2Offsets) ++ Seq(buildStableGroupRecordWithMember(
        generation, protocolType, protocol, segment2MemberId))).toArray: _*)
    val segment2End = expectGroupMetadataLoad(logMock, segment1End, segment2Records)

    EasyMock.expect(replicaManager.getLogEndOffset(groupTopicPartition)).andStubReturn(Some(segment2End))

    EasyMock.replay(logMock, replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)

    assertEquals(segment2MemberId, group.leaderOrNull, "segment2 group record member should be elected")
    assertEquals(Set(segment2MemberId), group.allMembers, "segment2 group record member should be only member")

    // offsets of segment1 should be overridden by segment2 offsets of the same topic partitions
    val committedOffsets = segment1Offsets ++ segment2Offsets
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testAddGroup(): Unit = {
    val group = new GroupMetadata("foo", Empty, time)
    assertEquals(group, groupMetadataManager.addGroup(group))
    assertEquals(group, groupMetadataManager.addGroup(new GroupMetadata("foo", Empty, time)))
  }

  @Test
  def testloadGroupWithStaticMember(): Unit = {
    val generation = 27
    val protocolType = "consumer"
    val staticMemberId = "staticMemberId"
    val dynamicMemberId = "dynamicMemberId"

    val staticMember = new MemberMetadata(staticMemberId, Some(groupInstanceId), "", "", rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))

    val dynamicMember = new MemberMetadata(dynamicMemberId, None, "", "", rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))

    val members = Seq(staticMember, dynamicMember)

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None, members, time)

    assertTrue(group.is(Empty))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertTrue(group.has(staticMemberId))
    assertTrue(group.has(dynamicMemberId))
    assertTrue(group.hasStaticMember(groupInstanceId))
    assertEquals(Some(staticMemberId), group.currentStaticMemberId(groupInstanceId))
  }

  @Test
  def testLoadConsumerGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"
    val protocol = "protocol"
    val memberId = "member1"
    val topic = "foo"

    val subscriptions = List(
      ("protocol", ConsumerProtocol.serializeSubscription(new Subscription(List(topic).asJava)).array())
    )

    val member = new MemberMetadata(memberId, Some(groupInstanceId), "", "", rebalanceTimeout,
      sessionTimeout, protocolType, subscriptions)

    val members = Seq(member)

    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol, null, None,
      members, time)

    assertTrue(group.is(Stable))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Some(Set(topic)), group.getSubscribedTopics)
    assertTrue(group.has(memberId))
  }

  @Test
  def testLoadEmptyConsumerGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None,
      Seq(), time)

    assertTrue(group.is(Empty))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.protocolName.orNull)
    assertEquals(Some(Set.empty), group.getSubscribedTopics)
  }

  @Test
  def testLoadConsumerGroupWithFaultyConsumerProtocol(): Unit = {
    val generation = 27
    val protocolType = "consumer"
    val protocol = "protocol"
    val memberId = "member1"

    val subscriptions = List(("protocol", Array[Byte]()))

    val member = new MemberMetadata(memberId, Some(groupInstanceId), "", "", rebalanceTimeout,
      sessionTimeout, protocolType, subscriptions)

    val members = Seq(member)

    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol, null, None,
      members, time)

    assertTrue(group.is(Stable))
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(None, group.getSubscribedTopics)
    assertTrue(group.has(memberId))
  }

  @Test
  def testShouldThrowExceptionForUnsupportedGroupMetadataVersion(): Unit = {
    val generation = 1
    val protocol = "range"
    val memberId = "memberId"
    val unsupportedVersion = Short.MinValue

    // put the unsupported version as the version value
    val groupMetadataRecordValue = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
      .value().putShort(unsupportedVersion)
    // reset the position to the starting position 0 so that it can read the data in correct order
    groupMetadataRecordValue.position(0)

    val e = assertThrows(classOf[IllegalStateException],
      () => GroupMetadataManager.readGroupMessageValue(groupId, groupMetadataRecordValue, time))
    assertEquals(s"Unknown group metadata message version: $unsupportedVersion", e.getMessage)
  }

  @Test
  def testCurrentStateTimestampForAllGroupMetadataVersions(): Unit = {
    val generation = 1
    val protocol = "range"
    val memberId = "memberId"

    for (apiVersion <- ApiVersion.allVersions) {
      val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, apiVersion = apiVersion)

      val deserializedGroupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, groupMetadataRecord.value(), time)
      // GROUP_METADATA_VALUE_SCHEMA_V2 or higher should correctly set the currentStateTimestamp
      if (apiVersion >= KAFKA_2_1_IV0)
        assertEquals(Some(time.milliseconds()), deserializedGroupMetadata.currentStateTimestamp,
          s"the apiVersion $apiVersion doesn't set the currentStateTimestamp correctly.")
      else
        assertTrue(deserializedGroupMetadata.currentStateTimestamp.isEmpty,
          s"the apiVersion $apiVersion should not set the currentStateTimestamp.")
    }
  }

  @Test
  def testReadFromOldGroupMetadata(): Unit = {
    val generation = 1
    val protocol = "range"
    val memberId = "memberId"
    val oldApiVersions = Array(KAFKA_0_9_0, KAFKA_0_10_1_IV0, KAFKA_2_1_IV0)

    for (apiVersion <- oldApiVersions) {
      val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, apiVersion = apiVersion)

      val deserializedGroupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, groupMetadataRecord.value(), time)
      assertEquals(groupId, deserializedGroupMetadata.groupId)
      assertEquals(generation, deserializedGroupMetadata.generationId)
      assertEquals(protocolType, deserializedGroupMetadata.protocolType.get)
      assertEquals(protocol, deserializedGroupMetadata.protocolName.orNull)
      assertEquals(1, deserializedGroupMetadata.allMembers.size)
      assertEquals(deserializedGroupMetadata.allMembers, deserializedGroupMetadata.allDynamicMembers)
      assertTrue(deserializedGroupMetadata.allMembers.contains(memberId))
      assertTrue(deserializedGroupMetadata.allStaticMembers.isEmpty)
    }
  }

  @Test
  def testStoreEmptyGroup(): Unit = {
    val generation = 27
    val protocolType = "consumer"

    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None, Seq.empty, time)
    groupMetadataManager.addGroup(group)

    val capturedRecords = expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(capturedRecords.hasCaptured)
    val records = capturedRecords.getValue()(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId))
        .records.asScala.toList
    assertEquals(1, records.size)

    val record = records.head
    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
    assertTrue(groupMetadata.is(Empty))
    assertEquals(generation, groupMetadata.generationId)
    assertEquals(Some(protocolType), groupMetadata.protocolType)
  }

  @Test
  def testStoreEmptySimpleGroup(): Unit = {
    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val capturedRecords = expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(capturedRecords.hasCaptured)

    assertTrue(capturedRecords.hasCaptured)
    val records = capturedRecords.getValue()(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId))
      .records.asScala.toList
    assertEquals(1, records.size)

    val record = records.head
    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
    assertTrue(groupMetadata.is(Empty))
    assertEquals(0, groupMetadata.generationId)
    assertEquals(None, groupMetadata.protocolType)
  }

  @Test
  def testStoreGroupErrorMapping(): Unit = {
    assertStoreGroupErrorMapping(Errors.NONE, Errors.NONE)
    assertStoreGroupErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.COORDINATOR_NOT_AVAILABLE)
    assertStoreGroupErrorMapping(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NOT_COORDINATOR)
    assertStoreGroupErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.UNKNOWN_SERVER_ERROR)
    assertStoreGroupErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertStoreGroupErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    EasyMock.reset(replicaManager)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    expectAppendMessage(appendError)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map.empty, callback)
    assertEquals(Some(expectedError), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testStoreNonEmptyGroup(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NONE), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testStoreNonEmptyGroupWhenCoordinatorHasMoved(): Unit = {
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andReturn(None)
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val group = new GroupMetadata(groupId, Empty, time)

    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", Array[Byte]())))
    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    EasyMock.replay(replicaManager)

    var maybeError: Option[Errors] = None
    def callback(error: Errors): Unit = {
      maybeError = Some(error)
    }

    groupMetadataManager.storeGroup(group, Map(memberId -> Array[Byte]()), callback)
    assertEquals(Some(Errors.NOT_COORDINATOR), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffset(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset, "", time.milliseconds()))

    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NONE), maybeError)
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition)))
    val maybePartitionResponse = cachedOffsets.get(topicPartition)
    assertFalse(maybePartitionResponse.isEmpty)

    val partitionResponse = maybePartitionResponse.get
    assertEquals(Errors.NONE, partitionResponse.error)
    assertEquals(offset, partitionResponse.offset)

    EasyMock.verify(replicaManager)
    // Will update sensor after commit
    assertEquals(1, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testTransactionalCommitOffsetCommitted(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsetAndMetadata = OffsetAndMetadata(offset, "", time.milliseconds())
    val offsets = immutable.Map(topicPartition -> offsetAndMetadata)

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback, producerId, producerEpoch)
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    assertFalse(group.allOffsets.isEmpty)
    assertEquals(Some(offsetAndMetadata), group.offset(topicPartition))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testTransactionalCommitOffsetAppendFailure(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset, "", time.milliseconds()))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback, producerId, producerEpoch)
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NOT_ENOUGH_REPLICAS, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testTransactionalCommitOffsetAborted(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37
    val producerId = 232L
    val producerEpoch = 0.toShort

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset, "", time.milliseconds()))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback, producerId, producerEpoch)
    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertTrue(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertFalse(group.hasOffsets)
    assertTrue(group.allOffsets.isEmpty)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffsetWhenCoordinatorHasMoved(): Unit = {
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andReturn(None)
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset, "", time.milliseconds()))

    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.NOT_COORDINATOR), maybeError)

    EasyMock.verify(replicaManager)
  }

  @Test
  def testCommitOffsetFailure(): Unit = {
    assertCommitOffsetErrorMapping(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, Errors.COORDINATOR_NOT_AVAILABLE)
    assertCommitOffsetErrorMapping(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NOT_COORDINATOR)
    assertCommitOffsetErrorMapping(Errors.MESSAGE_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.RECORD_LIST_TOO_LARGE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.INVALID_FETCH_SIZE, Errors.INVALID_COMMIT_OFFSET_SIZE)
    assertCommitOffsetErrorMapping(Errors.CORRUPT_MESSAGE, Errors.CORRUPT_MESSAGE)
  }

  private def assertCommitOffsetErrorMapping(appendError: Errors, expectedError: Errors): Unit = {
    EasyMock.reset(replicaManager)

    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(topicPartition -> OffsetAndMetadata(offset, "", time.milliseconds()))

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(appendError, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(expectedError), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition).map(_.offset))

    EasyMock.verify(replicaManager)
    // Will not update sensor if failed
    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testCommitOffsetPartialFailure(): Unit = {
    EasyMock.reset(replicaManager)

    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val topicPartitionFailed = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(
      topicPartition -> OffsetAndMetadata(offset, "", time.milliseconds()),
      // This will failed
      topicPartitionFailed -> OffsetAndMetadata(offset, "s" * (offsetConfig.maxMetadataSize + 1) , time.milliseconds())
    )

    val capturedResponseCallback = appendAndCaptureCallback()
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)
    capturedResponseCallback.getValue.apply(Map(groupTopicPartition ->
      new PartitionResponse(Errors.NONE, 0L, RecordBatch.NO_TIMESTAMP, 0L)))

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition))
    assertEquals(Some(Errors.OFFSET_METADATA_TOO_LARGE), commitErrors.get.get(topicPartitionFailed))
    assertTrue(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition, topicPartitionFailed)))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartitionFailed).map(_.offset))

    EasyMock.verify(replicaManager)
    assertEquals(1, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testOffsetMetadataTooLarge(): Unit = {
    val memberId = ""
    val topicPartition = new TopicPartition("foo", 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)
    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val offsets = immutable.Map(
      topicPartition -> OffsetAndMetadata(offset, "s" * (offsetConfig.maxMetadataSize + 1) , time.milliseconds())
    )

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertFalse(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    val maybeError = commitErrors.get.get(topicPartition)
    assertEquals(Some(Errors.OFFSET_METADATA_TOO_LARGE), maybeError)
    assertFalse(group.hasOffsets)

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition).map(_.offset))

    assertEquals(0, TestUtils.totalMetricValue(metrics, "offset-commit-count"))
  }

  @Test
  def testExpireOffset(): Unit = {
    val memberId = ""
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, "", startMs, startMs + 1),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // expire only one of the offsets
    time.sleep(2)

    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(Some(offset), group.offset(topicPartition2).map(_.offset))

    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition2).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testGroupMetadataRemoval(): Unit = {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    mockGetPartition()
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(replicaManager, partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    val records = recordsCapture.getValue.records.asScala.toList
    recordsCapture.getValue.batches.forEach { batch =>
      assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
    }
    assertEquals(1, records.size)

    val metadataTombstone = records.head
    assertTrue(metadataTombstone.hasKey)
    assertFalse(metadataTombstone.hasValue)
    assertTrue(metadataTombstone.timestamp > 0)

    val groupKey = GroupMetadataManager.readMessageKey(metadataTombstone.key).asInstanceOf[GroupMetadataKey]
    assertEquals(groupId, groupKey.key)

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testGroupMetadataRemovalWithLogAppendTime(): Unit = {
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)
    group.generationId = 5

    // expect the group metadata tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    mockGetPartition()
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(replicaManager, partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    val records = recordsCapture.getValue.records.asScala.toList
    recordsCapture.getValue.batches.forEach { batch =>
      assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic)
      // Use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
    }
    assertEquals(1, records.size)

    val metadataTombstone = records.head
    assertTrue(metadataTombstone.hasKey)
    assertFalse(metadataTombstone.hasValue)
    assertTrue(metadataTombstone.timestamp > 0)

    val groupKey = GroupMetadataManager.readMessageKey(metadataTombstone.key).asInstanceOf[GroupMetadataKey]
    assertEquals(groupId, groupKey.key)

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
  }

  @Test
  def testExpireGroupWithOffsetsOnly(): Unit = {
    // verify that the group is removed properly, but no tombstone is written if
    // this is a group which is only using kafka for offset storage

    val memberId = ""
    val topicPartition1 = new TopicPartition("foo", 0)
    val topicPartition2 = new TopicPartition("foo", 1)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 millisecond
    val startMs = time.milliseconds
    val offsets = immutable.Map(
      topicPartition1 -> OffsetAndMetadata(offset, Optional.empty(), "", startMs, Some(startMs + 1)),
      topicPartition2 -> OffsetAndMetadata(offset, "", startMs, startMs + 3))

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // expire all of the offsets
    time.sleep(4)

    // expect the offset tombstone
    EasyMock.reset(partition)
    val recordsCapture: Capture[MemoryRecords] = EasyMock.newCapture()

    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.capture(recordsCapture),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    assertTrue(recordsCapture.hasCaptured)

    // verify the tombstones are correct and only for the expired offsets
    val records = recordsCapture.getValue.records.asScala.toList
    assertEquals(2, records.size)
    records.foreach { message =>
      assertTrue(message.hasKey)
      assertFalse(message.hasValue)
      val offsetKey = GroupMetadataManager.readMessageKey(message.key).asInstanceOf[OffsetKey]
      assertEquals(groupId, offsetKey.key.group)
      assertEquals("foo", offsetKey.key.topicPartition.topic)
    }

    // the full group should be gone since all offsets were removed
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    val cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))

    EasyMock.verify(replicaManager)
  }

  @Test
  def testOffsetExpirationSemantics(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"
    val topic = "foo"
    val topicPartition1 = new TopicPartition(topic, 0)
    val topicPartition2 = new TopicPartition(topic, 1)
    val topicPartition3 = new TopicPartition(topic, 2)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    val subscription = new Subscription(List(topic).asJava)
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeout, sessionTimeout,
      protocolType, List(("protocol", ConsumerProtocol.serializeSubscription(subscription).array())))
    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    val startMs = time.milliseconds
    // old clients, expiry timestamp is explicitly set
    val tp1OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs, startMs + 1)
    val tp2OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs, startMs + 3)
    // new clients, no per-partition expiry timestamp, offsets of group expire together
    val tp3OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs)
    val offsets = immutable.Map(
      topicPartition1 -> tp1OffsetAndMetadata,
      topicPartition2 -> tp2OffsetAndMetadata,
      topicPartition3 -> tp3OffsetAndMetadata)

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // do not expire any offset even though expiration timestamp is reached for one (due to group still being active)
    time.sleep(2)

    groupMetadataManager.cleanupGroupMetadata()

    // group and offsets should still be there
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(Some(tp1OffsetAndMetadata), group.offset(topicPartition1))
    assertEquals(Some(tp2OffsetAndMetadata), group.offset(topicPartition2))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicPartition3))

    var cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2, topicPartition3)))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition2).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition3).map(_.offset))

    EasyMock.verify(replicaManager)

    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // group is empty now, only one offset should expire
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(Some(tp2OffsetAndMetadata), group.offset(topicPartition2))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicPartition3))

    cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2, topicPartition3)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition2).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition3).map(_.offset))

    EasyMock.verify(replicaManager)

    time.sleep(2)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // one more offset should expire
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(None, group.offset(topicPartition2))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicPartition3))

    cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2, topicPartition3)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition3).map(_.offset))

    EasyMock.verify(replicaManager)

    // advance time to just before the offset of last partition is to be expired, no offset should expire
    time.sleep(group.currentStateTimestamp.get + defaultOffsetRetentionMs - time.milliseconds() - 1)

    groupMetadataManager.cleanupGroupMetadata()

    // one more offset should expire
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(None, group.offset(topicPartition2))
    assertEquals(Some(tp3OffsetAndMetadata), group.offset(topicPartition3))

    cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2, topicPartition3)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition3).map(_.offset))

    EasyMock.verify(replicaManager)

    // advance time enough for that last offset to expire
    time.sleep(2)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // group and all its offsets should be gone now
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))
    assertEquals(None, group.offset(topicPartition2))
    assertEquals(None, group.offset(topicPartition3))

    cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1, topicPartition2, topicPartition3)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition2).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition3).map(_.offset))

    EasyMock.verify(replicaManager)

    assert(group.is(Dead))
  }

  @Test
  def testOffsetExpirationOfSimpleConsumer(): Unit = {
    val memberId = "memberId"
    val topic = "foo"
    val topicPartition1 = new TopicPartition(topic, 0)
    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // expire the offset after 1 and 3 milliseconds (old clients) and after default retention (new clients)
    val startMs = time.milliseconds
    // old clients, expiry timestamp is explicitly set
    val tp1OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs)
    // new clients, no per-partition expiry timestamp, offsets of group expire together
    val offsets = immutable.Map(
      topicPartition1 -> tp1OffsetAndMetadata)

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topicPartition1))

    // do not expire offsets while within retention period since commit timestamp
    val expiryTimestamp = offsets(topicPartition1).commitTimestamp + defaultOffsetRetentionMs
    time.sleep(expiryTimestamp - time.milliseconds() - 1)

    groupMetadataManager.cleanupGroupMetadata()

    // group and offsets should still be there
    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assertEquals(Some(tp1OffsetAndMetadata), group.offset(topicPartition1))

    var cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1)))
    assertEquals(Some(offset), cachedOffsets.get(topicPartition1).map(_.offset))

    EasyMock.verify(replicaManager)

    // advance time to enough for offsets to expire
    time.sleep(2)

    // expect the offset tombstone
    EasyMock.reset(partition)
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    // group and all its offsets should be gone now
    assertEquals(None, groupMetadataManager.getGroup(groupId))
    assertEquals(None, group.offset(topicPartition1))

    cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topicPartition1)))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topicPartition1).map(_.offset))

    EasyMock.verify(replicaManager)

    assert(group.is(Dead))
  }

  @Test
  def testOffsetExpirationOfActiveGroupSemantics(): Unit = {
    val memberId = "memberId"
    val clientId = "clientId"
    val clientHost = "localhost"

    val topic1 = "foo"
    val topic1Partition0 = new TopicPartition(topic1, 0)
    val topic1Partition1 = new TopicPartition(topic1, 1)

    val topic2 = "bar"
    val topic2Partition0 = new TopicPartition(topic2, 0)
    val topic2Partition1 = new TopicPartition(topic2, 1)

    val offset = 37

    groupMetadataManager.addPartitionOwnership(groupPartitionId)

    val group = new GroupMetadata(groupId, Empty, time)
    groupMetadataManager.addGroup(group)

    // Subscribe to topic1 and topic2
    val subscriptionTopic1AndTopic2 = new Subscription(List(topic1, topic2).asJava)

    val member = new MemberMetadata(
      memberId,
      Some(groupInstanceId),
      clientId,
      clientHost,
      rebalanceTimeout,
      sessionTimeout,
      ConsumerProtocol.PROTOCOL_TYPE,
      List(("protocol", ConsumerProtocol.serializeSubscription(subscriptionTopic1AndTopic2).array()))
    )

    group.add(member, _ => ())
    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()
    group.transitionTo(Stable)

    val startMs = time.milliseconds

    val t1p0OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs)
    val t1p1OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs)

    val t2p0OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs)
    val t2p1OffsetAndMetadata = OffsetAndMetadata(offset, "", startMs)

    val offsets = immutable.Map(
      topic1Partition0 -> t1p0OffsetAndMetadata,
      topic1Partition1 -> t1p1OffsetAndMetadata,
      topic2Partition0 -> t2p0OffsetAndMetadata,
      topic2Partition1 -> t2p1OffsetAndMetadata)

    mockGetPartition()
    expectAppendMessage(Errors.NONE)
    EasyMock.replay(replicaManager)

    var commitErrors: Option[immutable.Map[TopicPartition, Errors]] = None
    def callback(errors: immutable.Map[TopicPartition, Errors]): Unit = {
      commitErrors = Some(errors)
    }

    groupMetadataManager.storeOffsets(group, memberId, offsets, callback)
    assertTrue(group.hasOffsets)

    assertFalse(commitErrors.isEmpty)
    assertEquals(Some(Errors.NONE), commitErrors.get.get(topic1Partition0))

    // advance time to just after the offset of last partition is to be expired
    time.sleep(defaultOffsetRetentionMs + 2)

    // no offset should expire because all topics are actively consumed
    groupMetadataManager.cleanupGroupMetadata()

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assert(group.is(Stable))

    assertEquals(Some(t1p0OffsetAndMetadata), group.offset(topic1Partition0))
    assertEquals(Some(t1p1OffsetAndMetadata), group.offset(topic1Partition1))
    assertEquals(Some(t2p0OffsetAndMetadata), group.offset(topic2Partition0))
    assertEquals(Some(t2p1OffsetAndMetadata), group.offset(topic2Partition1))

    var cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topic1Partition0, topic1Partition1, topic2Partition0, topic2Partition1)))

    assertEquals(Some(offset), cachedOffsets.get(topic1Partition0).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topic1Partition1).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topic2Partition0).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topic2Partition1).map(_.offset))

    EasyMock.verify(replicaManager)

    group.transitionTo(PreparingRebalance)

    // Subscribe to topic1, offsets of topic2 should be removed
    val subscriptionTopic1 = new Subscription(List(topic1).asJava)

    group.updateMember(
      member,
      List(("protocol", ConsumerProtocol.serializeSubscription(subscriptionTopic1).array())),
      null
    )

    group.initNextGeneration()
    group.transitionTo(Stable)

    // expect the offset tombstone
    EasyMock.expect(partition.appendRecordsToLeader(EasyMock.anyObject(classOf[MemoryRecords]),
      origin = EasyMock.eq(AppendOrigin.Coordinator), requiredAcks = EasyMock.anyInt(),
      EasyMock.anyObject())).andReturn(LogAppendInfo.UnknownLogAppendInfo)
    EasyMock.expectLastCall().times(1)

    EasyMock.replay(partition)

    groupMetadataManager.cleanupGroupMetadata()

    EasyMock.verify(partition)
    EasyMock.verify(replicaManager)

    assertEquals(Some(group), groupMetadataManager.getGroup(groupId))
    assert(group.is(Stable))

    assertEquals(Some(t1p0OffsetAndMetadata), group.offset(topic1Partition0))
    assertEquals(Some(t1p1OffsetAndMetadata), group.offset(topic1Partition1))
    assertEquals(None, group.offset(topic2Partition0))
    assertEquals(None, group.offset(topic2Partition1))

    cachedOffsets = groupMetadataManager.getOffsets(groupId, defaultRequireStable, Some(Seq(topic1Partition0, topic1Partition1, topic2Partition0, topic2Partition1)))

    assertEquals(Some(offset), cachedOffsets.get(topic1Partition0).map(_.offset))
    assertEquals(Some(offset), cachedOffsets.get(topic1Partition1).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topic2Partition0).map(_.offset))
    assertEquals(Some(OffsetFetchResponse.INVALID_OFFSET), cachedOffsets.get(topic2Partition1).map(_.offset))
  }

  @Test
  def testLoadOffsetFromOldCommit(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val apiVersion = KAFKA_1_1_IV0
    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, apiVersion = apiVersion, retentionTimeOpt = Some(100))
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, apiVersion = apiVersion)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestamp).get.nonEmpty)
    }
  }

  @Test
  def testLoadOffsetWithExplicitRetention(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val startOffset = 15L
    val groupEpoch = 2
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, retentionTimeOpt = Some(100))
    val memberId = "98098230493"
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Stable, group.currentState)
    assertEquals(memberId, group.leaderOrNull)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertEquals(protocol, group.protocolName.orNull)
    assertEquals(Set(memberId), group.allMembers)
    assertEquals(committedOffsets.size, group.allOffsets.size)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
      assertTrue(group.offset(topicPartition).map(_.expireTimestamp).get.nonEmpty)
    }
  }

  @Test
  def testSerdeOffsetCommitValue(): Unit = {
    val offsetAndMetadata = OffsetAndMetadata(
      offset = 537L,
      leaderEpoch = Optional.of(15),
      metadata = "metadata",
      commitTimestamp = time.milliseconds(),
      expireTimestamp = None)

    def verifySerde(apiVersion: ApiVersion, expectedOffsetCommitValueVersion: Int): Unit = {
      val bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, apiVersion)
      val buffer = ByteBuffer.wrap(bytes)

      assertEquals(expectedOffsetCommitValueVersion, buffer.getShort(0).toInt)

      val deserializedOffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
      assertEquals(offsetAndMetadata.offset, deserializedOffsetAndMetadata.offset)
      assertEquals(offsetAndMetadata.metadata, deserializedOffsetAndMetadata.metadata)
      assertEquals(offsetAndMetadata.commitTimestamp, deserializedOffsetAndMetadata.commitTimestamp)

      // Serialization drops the leader epoch silently if an older inter-broker protocol is in use
      val expectedLeaderEpoch = if (expectedOffsetCommitValueVersion >= 3)
        offsetAndMetadata.leaderEpoch
      else
        Optional.empty()

      assertEquals(expectedLeaderEpoch, deserializedOffsetAndMetadata.leaderEpoch)
    }

    for (version <- ApiVersion.allVersions) {
      val expectedSchemaVersion = version match {
        case v if v < KAFKA_2_1_IV0 => 1
        case v if v < KAFKA_2_1_IV1 => 2
        case _ => 3
      }
      verifySerde(version, expectedSchemaVersion)
    }
  }

  @Test
  def testSerdeOffsetCommitValueWithExpireTimestamp(): Unit = {
    // If expire timestamp is set, we should always use version 1 of the offset commit
    // value schema since later versions do not support it

    val offsetAndMetadata = OffsetAndMetadata(
      offset = 537L,
      leaderEpoch = Optional.empty(),
      metadata = "metadata",
      commitTimestamp = time.milliseconds(),
      expireTimestamp = Some(time.milliseconds() + 1000))

    def verifySerde(apiVersion: ApiVersion): Unit = {
      val bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, apiVersion)
      val buffer = ByteBuffer.wrap(bytes)
      assertEquals(1, buffer.getShort(0).toInt)

      val deserializedOffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
      assertEquals(offsetAndMetadata, deserializedOffsetAndMetadata)
    }

    for (version <- ApiVersion.allVersions)
      verifySerde(version)
  }

  @Test
  def testSerdeOffsetCommitValueWithNoneExpireTimestamp(): Unit = {
    val offsetAndMetadata = OffsetAndMetadata(
      offset = 537L,
      leaderEpoch = Optional.empty(),
      metadata = "metadata",
      commitTimestamp = time.milliseconds(),
      expireTimestamp = None)

    def verifySerde(apiVersion: ApiVersion): Unit = {
      val bytes = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, apiVersion)
      val buffer = ByteBuffer.wrap(bytes)
      val version = buffer.getShort(0).toInt
      if (apiVersion < KAFKA_2_1_IV0)
        assertEquals(1, version)
      else if (apiVersion < KAFKA_2_1_IV1)
        assertEquals(2, version)
      else
        assertEquals(3, version)

      val deserializedOffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(buffer)
      assertEquals(offsetAndMetadata, deserializedOffsetAndMetadata)
    }

    for (version <- ApiVersion.allVersions)
      verifySerde(version)
  }

  @Test
  def testLoadOffsetsWithEmptyControlBatch(): Unit = {
    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val generation = 15
    val groupEpoch = 2

    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildEmptyGroupRecord(generation, protocolType)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    // Prepend empty control batch to valid records
    val mockBatch: MutableRecordBatch = EasyMock.createMock(classOf[MutableRecordBatch])
    EasyMock.expect(mockBatch.iterator).andReturn(Collections.emptyIterator[Record])
    EasyMock.expect(mockBatch.isControlBatch).andReturn(true)
    EasyMock.expect(mockBatch.isTransactional).andReturn(true)
    EasyMock.expect(mockBatch.nextOffset).andReturn(16L)
    EasyMock.replay(mockBatch)
    val mockRecords: MemoryRecords = EasyMock.createMock(classOf[MemoryRecords])
    EasyMock.expect(mockRecords.batches).andReturn((Iterable[MutableRecordBatch](mockBatch) ++ records.batches.asScala).asJava).anyTimes()
    EasyMock.expect(mockRecords.records).andReturn(records.records()).anyTimes()
    EasyMock.expect(mockRecords.sizeInBytes()).andReturn(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + records.sizeInBytes()).anyTimes()
    EasyMock.replay(mockRecords)

    val logMock: Log = EasyMock.mock(classOf[Log])
    EasyMock.expect(logMock.logStartOffset).andReturn(startOffset).anyTimes()
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset),
      maxLength = EasyMock.anyInt(),
      isolation = EasyMock.eq(FetchLogEnd),
      minOneMessage = EasyMock.eq(true)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), mockRecords))
    EasyMock.expect(replicaManager.getLog(groupMetadataTopicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(replicaManager.getLogEndOffset(groupMetadataTopicPartition)).andStubReturn(Some(18))
    EasyMock.replay(logMock)
    EasyMock.replay(replicaManager)

    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), 0L)

    // Empty control batch should not have caused the load to fail
    val group = groupMetadataManager.getGroup(groupId).getOrElse(throw new AssertionError("Group was not loaded into the cache"))
    assertEquals(groupId, group.groupId)
    assertEquals(Empty, group.currentState)
    assertEquals(generation, group.generationId)
    assertEquals(Some(protocolType), group.protocolType)
    assertNull(group.leaderOrNull)
    assertNull(group.protocolName.orNull)
    committedOffsets.foreach { case (topicPartition, offset) =>
      assertEquals(Some(offset), group.offset(topicPartition).map(_.offset))
    }
  }

  @Test
  def testCommittedOffsetParsing(): Unit = {
    val groupId = "group"
    val topicPartition = new TopicPartition("topic", 0)
    val offsetCommitRecord = TestUtils.records(Seq(
      new SimpleRecord(
        GroupMetadataManager.offsetCommitKey(groupId, topicPartition),
        GroupMetadataManager.offsetCommitValue(OffsetAndMetadata(35L, "", time.milliseconds()), ApiVersion.latestVersion)
      )
    )).records.asScala.head
    val (keyStringOpt, valueStringOpt) = GroupMetadataManager.formatRecordKeyAndValue(offsetCommitRecord)
    assertEquals(Some(s"offset_commit::group=$groupId,partition=$topicPartition"), keyStringOpt)
    assertEquals(Some("offset=35"), valueStringOpt)
  }

  @Test
  def testCommittedOffsetTombstoneParsing(): Unit = {
    val groupId = "group"
    val topicPartition = new TopicPartition("topic", 0)
    val offsetCommitRecord = TestUtils.records(Seq(
      new SimpleRecord(GroupMetadataManager.offsetCommitKey(groupId, topicPartition), null)
    )).records.asScala.head
    val (keyStringOpt, valueStringOpt) = GroupMetadataManager.formatRecordKeyAndValue(offsetCommitRecord)
    assertEquals(Some(s"offset_commit::group=$groupId,partition=$topicPartition"), keyStringOpt)
    assertEquals(Some("<DELETE>"), valueStringOpt)
  }

  @Test
  def testGroupMetadataParsingWithNullUserData(): Unit = {
    val generation = 935
    val protocolType = "consumer"
    val protocol = "range"
    val memberId = "98098230493"
    val assignmentBytes = Utils.toArray(ConsumerProtocol.serializeAssignment(
      new ConsumerPartitionAssignor.Assignment(List(new TopicPartition("topic", 0)).asJava, null)
    ))
    val groupMetadataRecord = TestUtils.records(Seq(
      buildStableGroupRecordWithMember(generation, protocolType, protocol, memberId, assignmentBytes)
    )).records.asScala.head
    val (keyStringOpt, valueStringOpt) = GroupMetadataManager.formatRecordKeyAndValue(groupMetadataRecord)
    assertEquals(Some(s"group_metadata::group=$groupId"), keyStringOpt)
    assertEquals(Some("{\"protocolType\":\"consumer\",\"protocol\":\"range\"," +
      "\"generationId\":935,\"assignment\":\"{98098230493=[topic-0]}\"}"), valueStringOpt)
  }

  @Test
  def testGroupMetadataTombstoneParsing(): Unit = {
    val groupId = "group"
    val groupMetadataRecord = TestUtils.records(Seq(
      new SimpleRecord(GroupMetadataManager.groupMetadataKey(groupId), null)
    )).records.asScala.head
    val (keyStringOpt, valueStringOpt) = GroupMetadataManager.formatRecordKeyAndValue(groupMetadataRecord)
    assertEquals(Some(s"group_metadata::group=$groupId"), keyStringOpt)
    assertEquals(Some("<DELETE>"), valueStringOpt)
  }

  private def appendAndCaptureCallback(): Capture[Map[TopicPartition, PartitionResponse] => Unit] = {
    val capturedArgument: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      internalTopicsAllowed = EasyMock.eq(true),
      origin = EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.anyObject().asInstanceOf[Map[TopicPartition, MemoryRecords]],
      EasyMock.capture(capturedArgument),
      EasyMock.anyObject().asInstanceOf[Option[ReentrantLock]],
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject())
    )
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    capturedArgument
  }

  private def expectAppendMessage(error: Errors): Capture[Map[TopicPartition, MemoryRecords]] = {
    val capturedCallback: Capture[Map[TopicPartition, PartitionResponse] => Unit] = EasyMock.newCapture()
    val capturedRecords: Capture[Map[TopicPartition, MemoryRecords]] = EasyMock.newCapture()
    EasyMock.expect(replicaManager.appendRecords(EasyMock.anyLong(),
      EasyMock.anyShort(),
      internalTopicsAllowed = EasyMock.eq(true),
      origin = EasyMock.eq(AppendOrigin.Coordinator),
      EasyMock.capture(capturedRecords),
      EasyMock.capture(capturedCallback),
      EasyMock.anyObject().asInstanceOf[Option[ReentrantLock]],
      EasyMock.anyObject(),
      EasyMock.anyObject(),
      EasyMock.anyObject())
    ).andAnswer(new IAnswer[Unit] {
      override def answer: Unit = capturedCallback.getValue.apply(
        Map(groupTopicPartition ->
          new PartitionResponse(error, 0L, RecordBatch.NO_TIMESTAMP, 0L)
        )
      )})
    EasyMock.expect(replicaManager.getMagic(EasyMock.anyObject())).andStubReturn(Some(RecordBatch.CURRENT_MAGIC_VALUE))
    capturedRecords
  }

  private def buildStableGroupRecordWithMember(generation: Int,
                                               protocolType: String,
                                               protocol: String,
                                               memberId: String,
                                               assignmentBytes: Array[Byte] = Array.emptyByteArray,
                                               apiVersion: ApiVersion = ApiVersion.latestVersion): SimpleRecord = {
    val memberProtocols = List((protocol, Array.emptyByteArray))
    val member = new MemberMetadata(memberId, Some(groupInstanceId), "clientId", "clientHost", 30000, 10000, protocolType, memberProtocols)
    val group = GroupMetadata.loadGroup(groupId, Stable, generation, protocolType, protocol, memberId,
      if (apiVersion >= KAFKA_2_1_IV0) Some(time.milliseconds()) else None, Seq(member), time)
    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map(memberId -> assignmentBytes), apiVersion)
    new SimpleRecord(groupMetadataKey, groupMetadataValue)
  }

  private def buildEmptyGroupRecord(generation: Int, protocolType: String): SimpleRecord = {
    val group = GroupMetadata.loadGroup(groupId, Empty, generation, protocolType, null, null, None, Seq.empty, time)
    val groupMetadataKey = GroupMetadataManager.groupMetadataKey(groupId)
    val groupMetadataValue = GroupMetadataManager.groupMetadataValue(group, Map.empty, ApiVersion.latestVersion)
    new SimpleRecord(groupMetadataKey, groupMetadataValue)
  }

  private def expectGroupMetadataLoad(groupMetadataTopicPartition: TopicPartition,
                                      startOffset: Long,
                                      records: MemoryRecords): Unit = {
    val logMock: Log =  EasyMock.mock(classOf[Log])
    EasyMock.expect(replicaManager.getLog(groupMetadataTopicPartition)).andStubReturn(Some(logMock))
    val endOffset = expectGroupMetadataLoad(logMock, startOffset, records)
    EasyMock.expect(replicaManager.getLogEndOffset(groupMetadataTopicPartition)).andStubReturn(Some(endOffset))
    EasyMock.replay(logMock)
  }

  /**
   * mock records into a mocked log
   *
   * @return the calculated end offset to be mocked into [[ReplicaManager.getLogEndOffset]]
   */
  private def expectGroupMetadataLoad(logMock: Log,
                                      startOffset: Long,
                                      records: MemoryRecords): Long = {
    val endOffset = startOffset + records.records.asScala.size
    val fileRecordsMock: FileRecords = EasyMock.mock(classOf[FileRecords])

    EasyMock.expect(logMock.logStartOffset).andStubReturn(startOffset)
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset),
      maxLength = EasyMock.anyInt(),
      isolation = EasyMock.eq(FetchLogEnd),
      minOneMessage = EasyMock.eq(true)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))

    EasyMock.expect(fileRecordsMock.sizeInBytes()).andStubReturn(records.sizeInBytes)

    val bufferCapture = EasyMock.newCapture[ByteBuffer]
    fileRecordsMock.readInto(EasyMock.capture(bufferCapture), EasyMock.anyInt())
    EasyMock.expectLastCall().andAnswer(new IAnswer[Unit] {
      override def answer: Unit = {
        val buffer = bufferCapture.getValue
        buffer.put(records.buffer.duplicate)
        buffer.flip()
      }
    })

    EasyMock.replay(fileRecordsMock)

    endOffset
  }

  private def createCommittedOffsetRecords(committedOffsets: Map[TopicPartition, Long],
                                           groupId: String = groupId,
                                           apiVersion: ApiVersion = ApiVersion.latestVersion,
                                           retentionTimeOpt: Option[Long] = None): Seq[SimpleRecord] = {
    committedOffsets.map { case (topicPartition, offset) =>
      val commitTimestamp = time.milliseconds()
      val offsetAndMetadata = retentionTimeOpt match {
        case Some(retentionTimeMs) =>
          val expirationTime = commitTimestamp + retentionTimeMs
          OffsetAndMetadata(offset, "", commitTimestamp, expirationTime)
        case None =>
          OffsetAndMetadata(offset, "", commitTimestamp)
      }
      val offsetCommitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
      val offsetCommitValue = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, apiVersion)
      new SimpleRecord(offsetCommitKey, offsetCommitValue)
    }.toSeq
  }

  private def mockGetPartition(): Unit = {
    EasyMock.expect(replicaManager.getPartition(groupTopicPartition)).andStubReturn(HostedPartition.Online(partition))
    EasyMock.expect(replicaManager.onlinePartition(groupTopicPartition)).andStubReturn(Some(partition))
  }

  private def getGauge(manager: GroupMetadataManager, name: String): Gauge[Int]  = {
    KafkaYammerMetrics.defaultRegistry().allMetrics().get(manager.metricName(name, Map.empty)).asInstanceOf[Gauge[Int]]
  }

  private def expectMetrics(manager: GroupMetadataManager,
                            expectedNumGroups: Int,
                            expectedNumGroupsPreparingRebalance: Int,
                            expectedNumGroupsCompletingRebalance: Int): Unit = {
    assertEquals(expectedNumGroups, getGauge(manager, "NumGroups").value)
    assertEquals(expectedNumGroupsPreparingRebalance, getGauge(manager, "NumGroupsPreparingRebalance").value)
    assertEquals(expectedNumGroupsCompletingRebalance, getGauge(manager, "NumGroupsCompletingRebalance").value)
  }

  @Test
  def testMetrics(): Unit = {
    groupMetadataManager.cleanupGroupMetadata()
    expectMetrics(groupMetadataManager, 0, 0, 0)
    val group = new GroupMetadata("foo2", Stable, time)
    groupMetadataManager.addGroup(group)
    expectMetrics(groupMetadataManager, 1, 0, 0)
    group.transitionTo(PreparingRebalance)
    expectMetrics(groupMetadataManager, 1, 1, 0)
    group.transitionTo(CompletingRebalance)
    expectMetrics(groupMetadataManager, 1, 0, 1)
  }

  @Test
  def testPartitionLoadMetric(): Unit = {
    val server = ManagementFactory.getPlatformMBeanServer
    val mBeanName = "kafka.server:type=group-coordinator-metrics"
    val reporter = new JmxReporter
    val metricsContext = new KafkaMetricsContext("kafka.server")
    reporter.contextChange(metricsContext)
    metrics.addReporter(reporter)

    def partitionLoadTime(attribute: String): Double = {
      server.getAttribute(new ObjectName(mBeanName), attribute).asInstanceOf[Double]
    }

    assertTrue(server.isRegistered(new ObjectName(mBeanName)))
    assertEquals(Double.NaN, partitionLoadTime( "partition-load-time-max"), 0)
    assertEquals(Double.NaN, partitionLoadTime("partition-load-time-avg"), 0)
    assertTrue(reporter.containsMbean(mBeanName))

    val groupMetadataTopicPartition = groupTopicPartition
    val startOffset = 15L
    val memberId = "98098230493"
    val committedOffsets = Map(
      new TopicPartition("foo", 0) -> 23L,
      new TopicPartition("foo", 1) -> 455L,
      new TopicPartition("bar", 0) -> 8992L
    )

    val offsetCommitRecords = createCommittedOffsetRecords(committedOffsets)
    val groupMetadataRecord = buildStableGroupRecordWithMember(generation = 15,
      protocolType = "consumer", protocol = "range", memberId)
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE,
      (offsetCommitRecords ++ Seq(groupMetadataRecord)).toArray: _*)

    expectGroupMetadataLoad(groupMetadataTopicPartition, startOffset, records)
    EasyMock.replay(replicaManager)

    // When passed a specific start offset, assert that the measured values are in excess of that.
    val now = time.milliseconds()
    val diff = 1000
    val groupEpoch = 2
    groupMetadataManager.loadGroupsAndOffsets(groupMetadataTopicPartition, groupEpoch, _ => (), now - diff)
    assertTrue(partitionLoadTime("partition-load-time-max") >= diff)
    assertTrue(partitionLoadTime("partition-load-time-avg") >= diff)
  }
}
