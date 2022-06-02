/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package kafka.api

import scala.collection.JavaConverters._

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.IsolationLevel

/**
 * Scalability/performance test for sequential vs. parallel controller startup, at least when run
 * manually with maxTopics set to 1500.  KafkaController doesn't expose its detailed timings outside
 * of logs, however, so the test case doesn't explicitly <em>verify</em> parallel startup, though it
 * does configure it explicitly.
 */
class ParallelControllerStartupTest extends MultiClusterAbstractConsumerTest {
  override def numClusters: Int = 1
  override def brokerCountPerCluster: Int = 3


  // Given brokerCountPerCluster = 3, set up brokers and controllers as follows:
  //    +---------------------------------------+
  //    | PHYSICAL CLUSTER(-INDEX) 0            |
  //    |  - broker 100 = preferred controller  |
  //    |  - broker 101 = data broker           |
  //    |  - broker 102 = data broker           |
  //    +---------------------------------------+
  // The bootstrap-servers list for each cluster will contain all 3 brokers.  (This is the minimal
  // setup with a preferred controller and topic replication across data brokers.)
  override def modifyConfigs(props: Seq[Properties], clusterIndex: Int): Unit = {
    debug(s"beginning ParallelControllerStartupTest modifyConfigs() override for clusterIndex=${clusterIndex}")
    super.modifyConfigs(props, clusterIndex)
    (0 until brokerCountPerCluster).map { brokerIndex =>
      // 100-102, 200-202, etc.
      val brokerId = 100*(clusterIndex + 1) + brokerIndex
      debug(s"clusterIndex=${clusterIndex}, brokerIndex=${brokerIndex}: setting broker.id=${brokerId}")
      props(brokerIndex).setProperty(KafkaConfig.BrokerIdProp, brokerId.toString)
      if (brokerIndex < 1) {
        // true for brokerIds that are a multiple of 100 only
        debug(s"clusterIndex=${clusterIndex}, brokerIndex=${brokerIndex}, broker.id=${brokerId}: setting preferred.controller=true")
        props(brokerIndex).setProperty(KafkaConfig.PreferredControllerProp, "true")
      } else {
        debug(s"clusterIndex=${clusterIndex}, brokerIndex=${brokerIndex}, broker.id=${brokerId}: leaving preferred.controller=false")
      }
    }
    debug(s"done with ParallelControllerStartupTest modifyConfigs() override for clusterIndex=${clusterIndex}\n\n\n\n\n")
  }


  @BeforeEach
  override def setUp(): Unit = {
    debug(s"beginning setUp() override for ParallelControllerStartupTest to disallow data brokers acting as controllers, disable auto-topic creation, compensate for 1970-era records, etc.\n\n\n\n\n")

    //this.serverConfig.setProperty(KafkaConfig.LiCombinedControlRequestEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.AllowPreferredControllerFallbackProp, "false")
    this.serverConfig.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, "false")
    this.serverConfig.setProperty(KafkaConfig.LogRetentionTimeMillisProp, "9999999999999") // 316 years, heh heh
    this.serverConfig.setProperty(KafkaConfig.LiNumControllerInitThreadsProp, "10")        // parallel-startup mode

    super.setUp()  // invokes modifyConfigs() shortly after starting ZK; eventually starts brokers, then elects controllers
    debug(s"done with setUp() override for ParallelControllerStartupTest\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  }


  @Test
  def testControllerParallelInit(): Unit = {
    debug(s"starting testControllerParallelInit()")
    val numRecords = 1000
    val producer = createProducer()
    debug(s"about to produce ${numRecords} records to ${tp1c0}")
    // NOTE:  this method uses the record index (0, 1, 2, ...) as the timestamp => ENTIRELY FROM JANUARY 1970:
    sendRecords(producer, numRecords, tp1c0)
    debug(s"done producing ${numRecords} records to ${tp1c0}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")


    debug(s"creating admin client for cluster 0")
    val cluster0AdminClient = createAdminClient(/* clusterIndex = 0 */)
    debug(s"requesting earliest and latest offsets for ${tp1c0} using admin client for cluster 0 ...")
    var earliestOffset = cluster0AdminClient.listOffsets(
      Map(tp1c0 -> OffsetSpec.earliest()).asJava,
      new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(10000, TimeUnit.MILLISECONDS).get(tp1c0).offset()
    var latestOffset = cluster0AdminClient.listOffsets(
      Map(tp1c0 -> OffsetSpec.latest()).asJava,
      new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(10000, TimeUnit.MILLISECONDS).get(tp1c0).offset()
    debug(s"before 1500 topics, start/end offsets for ${tp1c0} in cluster 0 are:  ${earliestOffset}-${latestOffset}")


    // Create a BUNCH more topics and partitions in cluster 0 so we can bounce the controller and get
    // accurate timing estimates for sequential vs. parallel startups.
    // [no particular reason for doing it here vs. before producer or after consumer]
    val maxTopics = 100
    //val maxTopics = 1500  // works, but test duration is ~6 minutes:  enable only for manual testing
    //val maxTopics = 5000  // blows up at 1613 topics due to gradle/JVM mem usage
    (0 until maxTopics).map { topicNum =>
      createTopic(f"topic_${topicNum}%05d", numPartitions = 10, replicationFactor = 2, clusterIndex = 0)
      if ((topicNum + 1) % 100 == 0) {
        debug(s"created ${topicNum + 1} of ${maxTopics} topics in cluster 0")
      }
    }

    debug(s"again requesting earliest and latest offsets for ${tp1c0} after ${maxTopics} created ...")
    earliestOffset = cluster0AdminClient.listOffsets(
      Map(tp1c0 -> OffsetSpec.earliest()).asJava,
      new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(10000, TimeUnit.MILLISECONDS).get(tp1c0).offset()
    latestOffset = cluster0AdminClient.listOffsets(
      Map(tp1c0 -> OffsetSpec.latest()).asJava,
      new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(10000, TimeUnit.MILLISECONDS).get(tp1c0).offset()
    debug(s"after 1500 topics, start/end offsets for ${tp1c0} in cluster 0 are:  ${earliestOffset}-${latestOffset}")


    // Now "find" initial controller for cluster 0 and its epoch, then force it to restart by deleting
    // the controller znode in ZK.  (Technically we should verify that all topics, or at least the last
    // few, are online, but it's probably OK to let the restarted controller handle that.)
    val initialController = serversByCluster(0).find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    val initialEpoch = initialController.epoch

    // delete the controller znode in cluster 0 so that a new controller can be elected...
    debug(s"forcing election of new controller by nuking 'controller' znode in cluster 0")
    zkClient(0).deleteController(initialController.controllerContext.epochZkVersion)
    // ...and wait until a new controller has been elected
    TestUtils.waitUntilTrue(() => {
      serversByCluster(0).exists { server =>
        server.kafkaController.isActive && server.kafkaController.epoch > initialEpoch
      }
    }, "Failed to find newly elected controller")


    debug(s"yet again requesting earliest and latest offsets for ${tp1c0} after controller bounced ...")
    earliestOffset = cluster0AdminClient.listOffsets(
      Map(tp1c0 -> OffsetSpec.earliest()).asJava,
      new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(10000, TimeUnit.MILLISECONDS).get(tp1c0).offset()
    latestOffset = cluster0AdminClient.listOffsets(
      Map(tp1c0 -> OffsetSpec.latest()).asJava,
      new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get(10000, TimeUnit.MILLISECONDS).get(tp1c0).offset()
    debug(s"after controller failover, start/end offsets for ${tp1c0} in cluster 0 are:  ${earliestOffset}-${latestOffset}")

    // ideally would measure startup time or something here, but for now inspection of logs after the fact is fine

    debug(s"about to create consumer and read ${numRecords} records from ${tp1c0}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
    val consumer = createConsumer()
    consumer.assign(List(tp1c0).asJava)
    consumer.seek(tp1c0, 0)
    consumeAndVerifyRecords(consumer = consumer, numRecords = numRecords, startingOffset = 0)

    debug(s"done with testControllerParallelInit(), apparently successfully!\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
  }

}
