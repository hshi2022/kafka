/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.controller.PartitionLeaderElectionAlgorithms.OfflineElectionResult
import kafka.server.OffsetAndEpoch
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

import scala.collection.{Map, Seq}

case class ElectionResult(topicPartition: TopicPartition, leaderAndIsr: Option[LeaderAndIsr], liveReplicas: Seq[Int])

object Election extends Logging {

  /**
   * @param allowUncleanCorruptedLeaders If enabled, allow an unclean corrupted broker to be elected leader.
   *                                     This is set to true when delayed election is turned off.
   * @return an election result, along with a flag indicating
   * whether a delayed election for a corrupt leader needs to be triggered.
   */
  private def leaderForOffline(partition: TopicPartition,
                               leaderAndIsrOpt: Option[LeaderAndIsr],
                               uncleanLeaderElectionEnabled: Boolean,
                               allowUncleanCorruptedLeaders: Boolean,
                               controllerContext: ControllerContext): (ElectionResult, Boolean) = {
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContextSnapshot.isReplicaOnline(replica, partition))
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        val isr = leaderAndIsr.isr

        def getIsrForLeader(leader: Int, uncleanElection: Boolean): ElectionResult = {
          if (uncleanElection) {
            controllerContext.stats.uncleanLeaderElectionRate.mark()
            warn(s"Unclean leader election. Partition $partition has been assigned leader $leader from deposed " +
              s"leader ${leaderAndIsr.leader}.")
          }
          val newIsr =
            if (isr.contains(leader)) {
              isr.filter(replica => controllerContextSnapshot.isReplicaOnline(replica, partition))
            } else {
              List(leader)
            }
          val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(leader, newIsr)
          ElectionResult(partition, Some(newLeaderAndIsr), liveReplicas)
        }

        val electionResult: OfflineElectionResult = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(
          assignment, isr, liveReplicas.toSet,
          controllerContext.corruptedBrokers.keys.toSet, uncleanLeaderElectionEnabled)

        electionResult match {
          case OfflineElectionResult.CleanLeader(leader) =>
            (getIsrForLeader(leader, uncleanElection = false), false)
          case OfflineElectionResult.UncleanLeader(leader) =>
            (getIsrForLeader(leader, uncleanElection = true), false)
          case OfflineElectionResult.CorruptedUncleanLeader(leader) =>
            if (allowUncleanCorruptedLeaders) (getIsrForLeader(leader, uncleanElection = true), false)
            else (ElectionResult(partition, None, liveReplicas), true)
          case OfflineElectionResult.NoLeader =>
            (ElectionResult(partition, None, liveReplicas), false)
        }
      case None =>
        (ElectionResult(partition, None, liveReplicas), false)
    }
  }

  /**
   * Elect leaders for new or offline partitions.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param partitionsWithUncleanLeaderElectionState A sequence of tuples representing the partitions
   *                                                 that need election, their leader/ISR state, and whether
   *                                                 or not unclean leader election is enabled
   *
   * @return The election results
   */
  def leaderForOffline(
    controllerContext: ControllerContext,
    partitionsWithUncleanLeaderElectionState: Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)],
    allowUncleanCorruptedLeaders: Boolean,
  ): Seq[(ElectionResult, Boolean)] = {
    partitionsWithUncleanLeaderElectionState.map {
      case (partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled) =>
        leaderForOffline(partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled,
          allowUncleanCorruptedLeaders, controllerContext)
    }
  }

  private def leaderForReassign(partition: TopicPartition,
                                leaderAndIsr: LeaderAndIsr,
                                controllerContext: ControllerContext): ElectionResult = {
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    val targetReplicas = controllerContext.partitionFullReplicaAssignment(partition).targetReplicas
    val liveReplicas = targetReplicas.filter(replica => controllerContextSnapshot.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(targetReplicas, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, targetReplicas)
  }

  /**
   * Elect leaders for partitions that are undergoing reassignment.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForReassign(controllerContext: ControllerContext,
                        leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForReassign(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForRecommendation(partition: TopicPartition,
    leaderAndIsr: LeaderAndIsr,
    recommendedLeader: Option[Int],
    controllerContext: ControllerContext,
    controllerContextSnapshot: ControllerContextSnapshot): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContextSnapshot.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.recommendedPartitionLeaderElection(recommendedLeader, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)
  }

  /**
   * Elect leaders for partitions that have a recommended leader.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   * @param recommendedLeaders A map from each partition to its recommended leader
   * @return The election results
   */
  def leaderForRecommendation(controllerContext: ControllerContext,
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    recommendedLeaders: Map[TopicPartition, Int]): Seq[ElectionResult] = {
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForRecommendation(partition, leaderAndIsr, recommendedLeaders.get(partition), controllerContext, controllerContextSnapshot)
    }
  }

  private def leaderForDelayedElection(partition: TopicPartition,
    leaderAndIsr: LeaderAndIsr,
    brokerIdToOffsetAndEpochOpt: Option[Map[Int, OffsetAndEpoch]],
    controllerContext: ControllerContext,
    controllerContextSnapshot: ControllerContextSnapshot): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContextSnapshot.isReplicaOnline(replica, partition))
    val leaderOpt = brokerIdToOffsetAndEpochOpt.flatMap(brokerIdToOffsetAndEpoch =>
      PartitionLeaderElectionAlgorithms.delayedPartitionLeaderElection(
        brokerIdToOffsetAndEpoch, assignment, liveReplicas.toSet))
    val isr = leaderAndIsr.isr
    val newLeaderAndIsrOpt = leaderOpt.map(leader => {
      val newIsr = if (isr.contains(leader)) isr.filter(replica =>
        controllerContextSnapshot.isReplicaOnline(replica, partition))
      else List(leader)
      leaderAndIsr.newLeaderAndIsr(leader, newIsr)
    })
    ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)
  }

  /**
   * Elect leaders for partitions that have a recommended leader.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   * @param recommendedLeaders A map from each partition to its recommended leader
   * @return The election results
   */
  def leaderForDelayedElection(controllerContext: ControllerContext,
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    brokerIdToOffsetMaps: Map[TopicPartition, Map[Int, OffsetAndEpoch]]): Seq[ElectionResult] = {
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForDelayedElection(
        partition, leaderAndIsr, brokerIdToOffsetMaps.get(partition), controllerContext, controllerContextSnapshot)
    }
  }

  private def leaderForPreferredReplica(partition: TopicPartition,
                                        leaderAndIsr: LeaderAndIsr,
                                        controllerContext: ControllerContext): ElectionResult = {
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContextSnapshot.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, assignment)
  }

  /**
   * Elect preferred leaders.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForPreferredReplica(controllerContext: ControllerContext,
                                leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForPreferredReplica(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForControlledShutdown(partition: TopicPartition,
                                          leaderAndIsr: LeaderAndIsr,
                                          shuttingDownBrokerIds: Set[Int],
                                          controllerContext: ControllerContext): ElectionResult = {
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveOrShuttingDownReplicas = assignment.filter(replica =>
      controllerContextSnapshot.isReplicaOnline(replica, partition, includeShuttingDownBrokers = true))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr,
      liveOrShuttingDownReplicas.toSet, shuttingDownBrokerIds)
    val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeaderAndIsr(leader, newIsr))
    ElectionResult(partition, newLeaderAndIsrOpt, liveOrShuttingDownReplicas)
  }

  /**
   * Elect leaders for partitions whose current leaders are shutting down.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForControlledShutdown(controllerContext: ControllerContext,
                                  leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    val shuttingDownBrokerIdSet = controllerContext.shuttingDownBrokerIds.keySet.toSet
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForControlledShutdown(partition, leaderAndIsr, shuttingDownBrokerIdSet, controllerContext)
    }
  }
}
